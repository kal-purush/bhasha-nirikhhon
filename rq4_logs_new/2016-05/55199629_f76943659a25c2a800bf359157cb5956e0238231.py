from five import grok
from plone.directives import dexterity, form
from eval.workspace.content.folder import IFolder

grok.templatedir('templates')

class Index(dexterity.DisplayForm):
    grok.context(IFolder)
    grok.require('zope2.View')
    grok.template('folder_view')
    grok.name('view')
