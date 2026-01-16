from five import grok
from plone.directives import dexterity, form
from Products.CMFCore.utils import getToolByName
from plone import api
from eval.workspace.content.workspace import IWorkspace

grok.templatedir('templates')

class my_documents_view(dexterity.DisplayForm):
    grok.context(IWorkspace)
    grok.require('zope2.View')
    
    @property
    def catalog(self):
        return getToolByName(self.context, 'portal_catalog')
    
    @property
    def membership(self):
        return getToolByName(self.context, 'portal_membership')        
    
    def contents(self):
        auth_user = self.context.portal_membership.getAuthenticatedMember().getUserName()
        results = []
        brains = self.context.portal_catalog({'path':{'query':'/'.join(self.context.getPhysicalPath()),
                                                      'depth':1},
                                                'portal_type':'eval.workspace.document_file'})
        for brain in brains:
            if brain.Creator == auth_user:
                results.append(brain)
        return results

    def roles(self, obj=None):
        current = str(api.user.get_current())
        roles = api.user.get_roles(username=current, obj= obj)
        allowed =  ['Owner','Manager', 'Administrator'] 
        return any((True for x in roles if x in allowed))

    def searchedValue(self, name=None):
        result = 0
        form = self.request.form
        if form.has_key('data'):
            result = form[name]
        return result

                
        