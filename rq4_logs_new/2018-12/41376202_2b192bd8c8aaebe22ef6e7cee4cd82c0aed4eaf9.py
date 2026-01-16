import grok
from zope.catalog.interfaces import ICatalog
from zope.component import queryUtility
from zope.intid.interfaces import IIntIds
from zope.authentication.interfaces import IAuthentication
from zope.pluggableauth.interfaces import (
    IAuthenticatorPlugin, ICredentialsPlugin)
from .components import


class Cataloger:

    def __init__(self, catalog, trigger):
        self.catalog = catalog  # grok.Indexes class
        self.trigger = trigger  # Event class

    @property
    def title(self):
        return 'Cataloger: %s' % catalog_name

    @property
    def status(self, site):
        catalog_name = grok.name.bind().get(self.catalog)
        catalog = queryUtility(ICatalog, name=catalog_name) is not None

    def get(self, site):
        sm = site.getSiteManager()
        name = grok.name.bind().get(self.component)
        return sm.queryUtility(self.provides, name=name)
        
    def install(self, site):
        grok.notify(self.trigger(site))

    def uninstall(self, site):
        sm = site.getSiteManager()
        catalog_name = grok.name.bind().get(self.catalog)
        catalog = sm.queryUtility(ICatalog, name=catalog_name)
        if catalog is not None:
            name_in_container = catalog.__name__
            if sm.unregisterUtility(
                    catalog, provided=ICatalog, name=catalog_name):
                del sm[name_in_container]
                return True
            raise uvcsite.plugins.PluginErrors(
                self.title,
                u'Catalog unregistration was unsuccessful.')
        raise uvcsite.plugins.PluginErrors(
            self.title,
            u'Catalog does not exist.')

    def recatalog(self, site, items_iterator):
        sm = site.getSiteManager()
        catalog_name = grok.name.bind().get(self.catalog)
        catalog = sm.getUtility(ICatalog, name=catalog_name)
        ids = sm.getUtility(IIntIds)

        counter = 0
        for obj in items_iterator:
            id = ids.queryId(obj)
            if id is not None:
                catalog.index_doc(id, obj)
                counter += 1

        return counter

    def diagnose(self, site):
        sm = site.getSiteManager()
        catalog_name = grok.name.bind().get(self.catalog)
        catalog = sm.getUtility(ICatalog, name=catalog_name)
        indexes = list(catalog.keys())
        return {
            'Plugin': self.title,
            'Indexes': ', '.join(indexes),
            'Catalog name': catalog_name,
            'Number of documents': catalog[indexes[0]].documentCount(),
        }


class PAUComponent:

    types = {
        'authenticator': (
            'authenticatorPlugins', IAuthenticatorPlugin)
        'credentials': (
            'credentialsPlugins', ICredentialsPlugin)
    }

    def __init__(self, component, type, local=False):
        self.component = component  # callable factory
        assert type in self.types
        self.type = type
        self.attribute, self.provides = self.types[type]
        self.local = local  # Needs to be persisted as a local utility

    @property
    def title(self):
        return 'PAUComponent: %s (%s)' % (self.component.__name__, self.type)

    @property
    def status(self):
        pau = queryUtility(IAuthentication)
        name = grok.name.bind().get(self.component)
        pau_available = name in getattr(pau, self.attribute)

        if self.local:
            sm = site.getSiteManager()
            sm_available = name in sm
        else:
            sm_available = False

        if (pau_available and (self.local == sm_available)):
            return uvcsite.plugins.INSTALLED

        if (pau_available or (self.local == sm_available)):
            return uvcsite.plugins.INCONSISTANT

        return uvcsite.plugins.NOT_INSTALLED

    def get(self, site):
        sm = site.getSiteManager()
        name = grok.name.bind().get(self.component)
        return sm.queryUtility(self.provides, name=name)

    def install(self, site):        
        pau = sm.queryUtility(IAuthentication)
        name = grok.name.bind().get(self.component)

        values = getattr(pau, self.attribute)
        if name not in values:
            setattr(pau, self.attribute, values + (name,))

        if self.local and (name not in sm):
            sm = site.getSiteManager()
            utility = self.component()
            sm[name] = utility
            if sm.registerUtility(
                utility, provided=IAuthenticatorPlugin, name=name):
                del sm[name]
            raise uvcsite.plugins.PluginErrors(
                self.title,
                u'Catalog unregistration was unsuccessful.')

        return True

    def uninstall(self, site):
        pau = sm.queryUtility(IAuthentication)
        name = grok.name.bind().get(self.component)

        if self.local and (name in sm):
            sm = site.getSiteManager()
            utility = sm[name]
            sm.unregisterUtility(
                utility, provided=IAuthenticatorPlugin, name=name)
            del sm[name]

        values = getattr(pau, self.attribute)
        setattr(pau, self.attribute, tuple((p for p in values if p != name)))
        return True