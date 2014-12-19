# Copyright 2009-2013 Murdoch University
#

from starcluster.clustersetup import ClusterSetup
from starcluster.logger import log


class Puppet(ClusterSetup):

    def __init__(self, manifest, modules=None):
	self.manifest = manifest
	log.debug("Puppet plugin init")
	log.debug("Manifest %s" % manifest)

    def _puppet(self, node):
        log.info("Applying puppet on %s" % node.alias)
        node.ssh.execute("time puppet apply /etc/puppet/manifests/%s" % self.manifest, silent=False) 

    def _install_librarian(self, node):
        log.info("Installing librarian-puppet on %s" % node.alias)
        node.apt_command("install git") 
        node.apt_command("install ruby-dev") 
        node.ssh.execute("time gem install librarian-puppet --no-rdoc --no-ri -v 1.3.2", silent=False) 

    def _librarian(self, node):
        log.info("Running librarian-puppet on %s" % node.alias)
        node.ssh.execute("cd /etc/puppet && time librarian-puppet install", silent=False) 

    def _update_puppet(self, master, node):
        log.info("Uploading puppet modules on %s" % node.alias)
        if node.is_master():
            master.ssh.put('./puppet', '/etc/')
        else:
            master.ssh.execute("rsync -qaz --delete /etc/puppet %s:/etc/" % node.alias, silent=False)

    def _update(self, master, node=None):
        if node == None:
            node = master
        self._update_puppet(master, node)
        self._install_librarian(node)
        self._librarian(node)
        self._puppet(node)

    def run(self, nodes, master, user, user_shell, volumes):
        # ensure master is up to date first
        self._update(master)

        # disable queues 
        for node in nodes:
            if node.is_master():
                continue
            node.ssh.execute('qmod -d all.q@%s' % node.alias)

        # now update nodes
        for node in nodes:
            if node.is_master():
                continue

            self._update(master, node)
            node.ssh.execute('qmod -e all.q@%s' % node.alias)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        node.ssh.execute('qmod -d all.q@%s' % node.alias)
        self._update(master, node)
        node.ssh.execute('qmod -e all.q@%s' % node.alias)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        pass
