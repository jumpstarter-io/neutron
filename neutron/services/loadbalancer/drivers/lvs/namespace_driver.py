# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2014 Jumpstarter AB
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# @author: Daniel Lundqvist, Jumpstarter
import os
import shutil
import socket
import re


import netaddr
from oslo.config import cfg
from eventlet import greenthread

from neutron.agent.common import config
from neutron.agent.linux import ip_lib
from neutron.agent.linux import utils
from neutron.common import exceptions
from neutron.common import utils as n_utils
from neutron.openstack.common import excutils
from neutron.openstack.common import importutils
from neutron.openstack.common import log as logging
from neutron.plugins.common import constants
from neutron.services.loadbalancer.agent import agent_device_driver
from neutron.services.loadbalancer import constants as lb_const
import ipvsadm

LOG = logging.getLogger(__name__)
NS_PREFIX = 'qlbaas-'
DRIVER_NAME = 'lvs_ns'

STATE_PATH_DEFAULT = '$state_path/lbaas'
USER_GROUP_DEFAULT = 'nogroup'

OPTS = [
    cfg.StrOpt(
        'loadbalancer_state_path',
        default=STATE_PATH_DEFAULT,
        help=_('Location to store config and state files'),
        deprecated_opts=[cfg.DeprecatedOpt('loadbalancer_state_path')],
    ),
    cfg.StrOpt(
        'user_group',
        default=USER_GROUP_DEFAULT,
        help=_('The user group'),
        deprecated_opts=[cfg.DeprecatedOpt('user_group')],
    )
]
cfg.CONF.register_opts(OPTS, 'lvs')

class LvsNSDriver(agent_device_driver.AgentDeviceDriver):
    def __init__(self, conf, plugin_rpc):
        self.conf = conf
        self.root_helper = config.get_root_helper(conf)
        self.state_path = conf.lvs.loadbalancer_state_path
        try:
            vif_driver = importutils.import_object(conf.interface_driver, conf)
        except ImportError:
            with excutils.save_and_reraise_exception():
                msg = (_('Error importing interface driver: %s')
                       % conf.lvs.interface_driver)
                LOG.error(msg)
        self.vif_driver = vif_driver
        self.plugin_rpc = plugin_rpc
        self.pool_to_port_id = {}

    @classmethod
    def get_name(cls):
        return DRIVER_NAME

    def create_vip(self, vip):
        self._refresh_instance(vip['pool_id'], clear = True)

    def update_vip(self, old_vip, vip):
        self._refresh_instance(vip['pool_id'])

    def delete_vip(self, vip):
        self.undeploy_instance(vip['pool_id'])

    def create_pool(self, pool):
        # nothing to do here because a pool needs a vip to be useful        
        pass

    def update_pool(self, old_pool, pool):
        self._refresh_instance(pool['id'])

    def delete_pool(self, pool):
        self.undeploy_instance(pool['id'])

    def create_member(self, member):
        self._refresh_instance(member['pool_id'])

    def update_member(self, old_member, member):
        self._refresh_instance(member['pool_id'])

    @n_utils.synchronized('lvs-driver')
    def delete_member(self, member):
        logical_config = self._get_logical_config(member['pool_id'])
        namespace = self._get_namespace(logical_config)
        self._del_server(namespace, logical_config['vip'], member)
        self._purge_service(namespace)

    def update_pool_health_monitor(self,
                                    old_health_monitor,
                                    health_monitor,
                                    pool_id):

        LOG.debug(_('\n\nupdate_pool_health_monitor, old_health_monitor: %s\nhealth_monitor: %s\npool_id: %s\n\n'),
                    pp.pformat(old_health_monitor), pp.pformat(health_monitor), pool_id)
        pass

    
    def create_pool_health_monitor(self,
                                    health_monitor,
                                    pool_id):
        """Driver may call the code below in order to update the status.
        self.plugin.update_pool_health_monitor(context,
                                               health_monitor["id"],
                                               pool_id,
                                               constants.ACTIVE)
        """
        LOG.debug(_('\n\ncreate_pool_health_monitor, health_monitor: %s\npool_id: %s\n\n'),
                    pp.pformat(health_monitor), pool_id)
        pass

    def delete_pool_health_monitor(self, health_monitor, pool_id):
        LOG.debug(_('\n\ndelete_pool_health_monitor, health_monitor: %s\npool_id: %s\n\n'),
                    pp.pformat(health_monitor), pool_id)
        pass
    
    def _plug(self, namespace, port, reuse_existing=True):
        self.plugin_rpc.plug_vip_port(port['id'])
        interface_name = self.vif_driver.get_device_name(Wrap(port))

        if ip_lib.device_exists(interface_name, self.root_helper, namespace):
            if not reuse_existing:
                raise exceptions.PreexistingDeviceFailure(
                    dev_name=interface_name
                )
        else:
            self.vif_driver.plug(
                port['network_id'],
                port['id'],
                interface_name,
                port['mac_address'],
                namespace=namespace
            )

        cidrs = [
            '%s/%s' % (ip['ip_address'],
                       netaddr.IPNetwork(ip['subnet']['cidr']).prefixlen)
            for ip in port['fixed_ips']
        ]
        self.vif_driver.init_l3(interface_name, cidrs, namespace=namespace)

        gw_ip = port['fixed_ips'][0]['subnet'].get('gateway_ip')
        if gw_ip:
            cmd = ['route', 'add', 'default', 'gw', gw_ip]
            ip_wrapper = ip_lib.IPWrapper(self.root_helper,
                                          namespace=namespace)
            ip_wrapper.netns.execute(cmd, check_exit_code=False)

    def _get_namespace(self, logical_config):
        pool_id = logical_config['pool']['id']
        return get_ns_name(pool_id)
    
    def _get_logical_config(self, pool_id):
        return self.plugin_rpc.get_logical_device(pool_id)
    
    def _refresh_instance(self, pool_id, clear = False):
        logical_config = self._get_logical_config(pool_id)
        self.deploy_instance(logical_config, clear = clear)
        
    def _execute(self, namespace, cmd):
        ns = ip_lib.IPWrapper(self.root_helper, namespace)
        return ns.netns.execute(cmd)
    
    def _lvs_rows(self, namespace):
        cmd = ['ipvsadm', '-Ln']
        return self._execute(namespace, cmd).split('\n')

    def _service_exists(self, namespace, service_address, service_port):
        output_rows = self._lvs_rows(namespace)
        service = service_address + ":" + str(service_port)
        for row in output_rows:
            if "TCP  " + service in row:
                LOG.debug(_('\n\n_service_exists, found service %s in namespace: %s\n\n'), service, namespace)
                return True
        return False

    def _add_service(self, namespace, logical_config, member):
        pool = logical_config['pool']
        vip = logical_config['vip']
        exists = self._service_exists(namespace, vip['address'], member['protocol_port'])
        session_persistence = vip['session_persistence']
        service_address = vip['address']
        service_port = member['protocol_port']
        service = service_address + ":" + str(service_port)
        # if it already exists we edit the service
        if exists:
            cmd = ['ipvsadm', '-E', '-t', service]
        else:
            cmd = ['ipvsadm', '-A', '-t', service]
        # choose balance scheduler
        if pool['lb_method'] == "ROUND_ROBIN":
            cmd.extend(['-s', 'wrr'])
        elif pool['lb_method'] == "SOURCE_IP":
            cmd.extend(['-s', 'sh'])
        else:
            cmd.extend(['-s', 'wlc'])

        if session_persistence is not None:
            cmd.append('-p')
        self._execute(namespace, cmd)
        # remember the pool<>port mapping
        self.pool_to_port_id[pool['id']] = vip['port']['id']
    
    def _del_service(self, namespace, service):
        service_address = service['address']
        service_port = service['port']
        service = service_address + ":" + str(service_port)
        cmd = ['ipvsadm', '-D', '-t', service]
        self._execute(namespace, cmd)
        
    def _add_services(self, namespace, logical_config):
        for member in logical_config['members']:
            self._add_service(namespace, logical_config, member)
            pass
    
        
    def _server_exists(self, namespace, server_address, server_port):
        output_rows = self._lvs_rows(namespace)
        service = server_address + ":" + str(server_port)
        for row in output_rows:
            if "-> " + service in row:
                return True
        return False

    def _add_server(self, namespace, vip, member):
        service_address = vip['address']
        service_port = member['protocol_port']
        service = service_address + ":" + str(service_port)
        server_address = member['address']
        server_port = member['protocol_port']
        server = server_address + ":" + str(server_port)
        if self._server_exists(namespace, server_address, server_port):
            cmd = ['ipvsadm', '-e', '-t', service, '-r', server, '-m']
        else:
            cmd = ['ipvsadm', '-a', '-t', service, '-r', server, '-m']
        self._execute(namespace, cmd)
        
    def _del_server(self, namespace, vip, member):
        service_address = vip['address']
        service_port = member['protocol_port']
        service = service_address + ":" + str(service_port)
        server_address = member['address']
        server_port = member['protocol_port']
        server = server_address + ":" + str(server_port)
        cmd = ['ipvsadm', '-d', '-t', service, '-r', server]
        self._execute(namespace, cmd)

    def _add_servers(self, namespace, logical_config):
        for member in logical_config['members']:
            self._add_server(namespace, logical_config['vip'], member)
            
    def _clear_all(self, namespace):
        self._execute(namespace, ['ipvsadm', '-C'])
    
    def _purge_service(self, namespace):
        ipvsadm_state = self._get_current_state(namespace)
        empty_services = [service for service in ipvsadm_state if len(service['realservers']) == 0]
        for service in empty_services:
            self._del_service(namespace, service)        

    @n_utils.synchronized('lvs-driver')
    def deploy_instance(self, logical_config, clear=True):
        # do actual deploy only if vip and pool are configured and active
        if (not logical_config or
                'vip' not in logical_config or
                (logical_config['vip']['status'] not in
                 constants.ACTIVE_PENDING_STATUSES) or
                not logical_config['vip']['admin_state_up'] or
                (logical_config['pool']['status'] not in
                 constants.ACTIVE_PENDING_STATUSES) or
                not logical_config['pool']['admin_state_up']):
            return
        
        LOG.debug(_("\n\n\n logical_config: \n\n %s \n\n\n"), pp.pformat(logical_config))
        namespace = self._get_namespace(logical_config)
        if clear:
            # plug interface for this pool
            self._plug(namespace, logical_config['vip']['port'])
            # start from scratch
            self._clear_all(namespace)
        # add services"
        self._add_services(namespace, logical_config)
        # add servers
        self._add_servers(namespace, logical_config)
        
    @n_utils.synchronized('lvs-driver')
    def undeploy_instance(self, pool_id):
        namespace = get_ns_name(pool_id)
        ns = ip_lib.IPWrapper(self.root_helper, namespace)
        self._clear_all(namespace)
        # unplug the ports
        if pool_id in self.pool_to_port_id:
            self._unplug(namespace, self.pool_to_port_id[pool_id])
        ns.garbage_collect_namespace()
        
    def _nc_poll(self, namespace, address, port, timeout = '1'):
        cmd = ['ip', 'netns', 'exec', namespace, 'nc', '-w', timeout, address, port]
        try:
            obj, cmd = utils.create_process(cmd)
            _stdout, _stderr = (obj.communicate())
            obj.stdin.close()
            LOG.debug(_('\n\n returncode: %s\n\n'), obj.returncode)
            return obj.returncode == 0
        except Exception, e:
            LOG.debug(_('\n\n e: %s\n\n'), pp.pformat(e))
        finally:
            # NOTE from Thorwald: race "fix" according to the openstack gang
            greenthread.sleep(0)
            return False
        
        
    def _get_server_stats(self, pool_id):
        logical_config = self._get_logical_config(pool_id)
        namespace = self._get_namespace(logical_config)
        health_monitors = logical_config['pool']['health_monitors']
        res = {}
        for member in logical_config['members']:
            is_active = True
            if len(health_monitors) > 0:
                is_active = self._nc_poll(namespace, member['address'], member['protocol_port'], '1')
            res[member['id']] = {
                lb_const.STATS_STATUS: (constants.INACTIVE
                                        if is_active
                                        else constants.ACTIVE),
                lb_const.STATS_HEALTH: '',
                lb_const.STATS_FAILED_CHECKS: (0
                                               if is_active
                                               else 5)
            }
        return res

    def get_stats(self, pool_id):
        LOG.debug(_('\n\n get_stats, pool_id: %s\n\n'), pool_id)
        pool_stats = {}
        pool_stats['members'] = self._get_server_stats(pool_id)
        return pool_stats
        
    def _get_current_state(self, namespace):
        rows = self._lvs_rows(namespace)
        parents = []
        for row in rows:
            if row is None or row == '':
                continue
            data = re.split(r' +', row)
            if data[0] != '':
                parent = {}
                parent['protocol'] = data[0]
                if parent['protocol'] in ('TCP', 'UDP'):
                    parent['address'] = data[1].split(':')[0]
                    parent['port'] = data[1].split(':')[1]
                else:
                    parent['address'] = data[1]
                parent['scheduler'] = data[2]
                if data[3] == 'persistent':
                    parent['persistent'] = True
                    parent['timeout'] = data[4]
                else:
                    parent['persistent'] = False
                    parent['timeout'] = data[3]             
                parent['realservers'] = []
                parents.append(parent)
            else:
                child={}
                child['address'] = data[2].split(':')[0]
                child['port'] = data[2].split(':')[1]
                child['forwarding'] = data[3]
                child['weight'] = data[4]
                child['active_conn'] = data[5]
                child['inactive_conn'] = data[6]
                parent['realservers'].append(child)
        return parents
    
    def _get_lvs_stats(self, namespace):
        cmd = ['ipvsadm', '-Ln', '--stats']
        rows = self._execute(namespace, cmd).split('\n')[3:]
        parents = []
        for row in rows:
            if row is None or row == '':
                continue
            data = re.split(r' +', row)
            if data[0] != '':
                parent = {}
                parent['protocol'] = data[0]
                if parent['protocol'] in ('TCP', 'UDP'):
                    parent['address'] = data[1].split(':')[0]
                    parent['port'] = data[1].split(':')[1]
                else:
                    parent['address'] = data[1]
                parent['conns'] = data[2]
                parent['bin'] = data[4]
                parent['bout'] = data[5]
                parent['realservers'] = []
                parents.append(parent)
            else:
                child={}
                child['address'] = data[2].split(':')[0]
                child['port'] = data[2].split(':')[1]
                child['conns'] = data[3]
                child['bin'] = data[5]
                child['bout'] = data[6]
                parent['realservers'].append(child)
        return parents

# NOTE (markmcclain) For compliance with interface.py which expects objects
class Wrap(object):
    """A light attribute wrapper for compatibility with the interface lib."""
    def __init__(self, d):
        self.__dict__.update(d)

    def __getitem__(self, key):
        return self.__dict__[key]

def get_ns_name(namespace_id):
    return NS_PREFIX + namespace_id


import pprint
pp = pprint.PrettyPrinter(indent=4)