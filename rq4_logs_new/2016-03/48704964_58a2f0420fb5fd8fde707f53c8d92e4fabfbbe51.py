import ldap
import configmanager

cm = configmanager.ConfigManager()
ldap_user = cm.ldap_user
ldap_pass = cm.ldap_pass
base_dn = cm.base_dn

class AD_Server:

	def __init__(self, server):
		self.name = server
		self.ldap_url = 'ldap://' + server
		self.last_query = None

	def execute(self, filter, attributes):
		l = ldap.initialize(self.ldap_url)
		l.set_option(ldap.OPT_REFERRALS, 0)
		l.bind(ldap_user, ldap_pass)
		l.result()
		results = l.search_s(base_dn,ldap.SCOPE_SUBTREE,filter,attributes)
		l.unbind()
		return results

	def _find_host(self, search_name):
		filter = "(&(objectCategory=computer)(name={0}*))".format(search_name)
		attributes = ['dNSHostname','name','operatingSystem']
		return self.execute(filter, attributes)

	def get_windows_hosts(self, attr=None):
		filter = "(&(operatingSystem=*))"
		attributes = ['OperatingSystem','operatingSystemVersion','dNSHostName','name']
		return self.execute(filter, attributes)

