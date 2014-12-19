# Copyright 2009-2014 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.
import posixpath

from starcluster import clustersetup
from starcluster.templates import slurm
from starcluster.logger import log


class SlurmPlugin(clustersetup.DefaultClusterSetup):
    SLURM_CONFDIR = '/etc/slurm-llnl'
    MUNGE_CONFDIR = '/etc/munge'
    MUNGE_DEFAULTS = '/etc/default/munge'
    SLURM_SERVICE = 'slurm-llnl'
    MUNGE_SERVICE = 'munge'

    def __init__(self, master_can_run_jobs=True, **kwargs):
        self._master_can_run_jobs = master_can_run_jobs
        super(SlurmPlugin, self).__init__(**kwargs)

    def _slurm_path(self, path):
        return posixpath.join(self.SLURM_CONFDIR, path)

    def get_worker_nodes(self):
        return [t for t in self._nodes if
                self._master_can_run_jobs or not t.is_master()]

    def _node_defs(self):
        lines = []
        for node in self.get_worker_nodes():
            lines.append(
                'NodeName=%s CPUs=%d State=UNKNOWN' % (
                    node.alias, node.num_processors))
        return '\n'.join(lines)

    def _partition_def(self):
        node_list = ','.join(n.alias for n in self.get_worker_nodes())
        return 'PartitionName=debug Nodes=%s ' \
            'Default=YES MaxTime=INFINITE State=UP' % (node_list)

    def _update_config(self):
        master = self._master
        slurm_conf = master.ssh.remote_file(
            self._slurm_path('slurm.conf'), "w")
        conf = slurm.conf_template % {
            'node_defs': self._node_defs(),
            'partition_def': self._partition_def()
        }
        slurm_conf.write(conf)
        slurm_conf.close()

    def _restart_slurm(self, node):
        node.ssh.execute('service %s restart' % (self.SLURM_SERVICE))

    def _start_munge(self, node):
        outf = node.ssh.remote_file(
            self.MUNGE_DEFAULTS, "w")
        outf.write(slurm.munge_defaults)
        outf.close()
        node.ssh.execute('service %s restart' % (self.MUNGE_SERVICE))

    def _start_services(self, node):
        self._start_munge(node)
        self._restart_slurm(node)

    def _setup_slurm(self):
        """
        Set up Slurm on StarCluster
        """
        master = self._master
        log.info("Creating MUNGE key")
        master.ssh.execute('create-munge-key')
        self._setup_nfs(
            self.nodes,
            export_paths=[self.SLURM_CONFDIR, self.MUNGE_CONFDIR],
            start_server=False)
        log.info("Configuring and starting Slurm")
        self._update_config()
        for node in [self._master] + self.nodes:
            self.pool.simple_job(
                self._start_services, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(self.nodes))

    def run(self, nodes, master, user, user_shell, volumes):
        log.info("Configuring Slurm...")
        self._nodes = nodes
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes
        self._setup_slurm()

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self._nodes = nodes
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes
        log.info("Adding %s to Slum" % node.alias)
        self._setup_nfs(
            nodes=[node],
            export_paths=[self.SLURM_CONFDIR, self.MUNGE_CONFDIR],
            start_server=False)
        self._start_munge(node)
        self._update_config()
        self._restart_slurm(master)
        self._restart_slurm(node)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        self._nodes = [t for t in nodes if t != node]
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes
        log.info("Removing %s from Slurm" % node.alias)
        self._remove_nfs_exports(node)
        self._update_config()
        self._restart_slurm(master)
