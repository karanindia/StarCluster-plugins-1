from starcluster.clustersetup import ClusterSetup
from starcluster.logger import log

import boto
from boto.route53.record import ResourceRecordSets

class Route53(ClusterSetup):

    def __init__(self, hosted_zone_id, domain):
	self.hosted_zone_id = hosted_zone_id
	self.domain = domain
	log.debug("Route53 plugin init")
	log.debug("hosted_zone_id: %s"%hosted_zone_id)
	log.debug("domain: %s"%domain)

    def run(self, nodes, master, user, user_shell, volumes):
	host_name = "%s" % (master.alias)
	self.update_dns(host_name, master.ip_address)
	host_name = "%s.int" % (master.alias)
	self.update_dns(host_name, master.private_ip_address)

    def update_dns(self, host_name, ip_address):
	ttl = 10
	host_name = ".".join([host_name, self.domain])
        conn = boto.connect_route53()

        response = conn.get_all_rrsets(self.hosted_zone_id, 'A', host_name, maxitems=1)
        if len(response):
            response = response[0]
            comment = "Starcluster route53 plugin deleted record for %s"%(host_name)
            changes = ResourceRecordSets(conn, self.hosted_zone_id, comment)
            change1 = changes.add_change("DELETE", host_name, 'A', response.ttl)
            for old_value in response.resource_records:
                change1.add_value(old_value)
            try:
                changes.commit()
                log.info(comment)
            except Exception as e:
                log.warning(e)

        comment = "Starcluster route53 plugin updated record for %s to %s"%(host_name, ip_address)
        changes = ResourceRecordSets(conn, self.hosted_zone_id, comment)
        change2 = changes.add_change("CREATE", host_name, 'A', ttl)
        change2.add_value(ip_address)
        try:
            changes.commit()
            log.info(comment)
        except Exception as e:
            log.warning(e)
