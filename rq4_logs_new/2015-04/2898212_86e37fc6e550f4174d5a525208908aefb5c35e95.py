from ftw.builder import Builder
from ftw.builder import create
from plone.portlets.interfaces import IPortletManager, IPortletRenderer
from ftw.dashboard.portlets.recentlymodified.browser import recentlymodified
from ftw.dashboard.portlets.recentlymodified.testing import FTW_RECENTLYMODIFIED_FUNCTIONAL_TESTING
from plone.app.controlpanel.security import ISecuritySchema
from plone.app.testing import login
from plone.app.testing import TEST_USER_NAME, TEST_USER_PASSWORD
from plone.registry.interfaces import IRegistry
from plone.testing.z2 import Browser
from Products.CMFCore.utils import getToolByName
from zope.component import getUtility, getMultiAdapter
import transaction
import unittest2 as unittest


class TestExcludedMembersFolder(unittest.TestCase):

    layer = FTW_RECENTLYMODIFIED_FUNCTIONAL_TESTING

    def renderer(self, section=''):
        context = self.layer['portal']
        request = self.layer['request']
        view = context.restrictedTraverse('@@plone')
        manager = getUtility(IPortletManager, name='plone.rightcolumn',
                             context=context)
        assignment = recentlymodified.Assignment(section=section)

        renderer = getMultiAdapter(
            (context, request, view, manager, assignment), IPortletRenderer)
        return renderer

    def test_home_folder_contents_are_not_listed_in_portlet(self):
        portal = self.layer['portal']

        # Enable user folders.
        security_adapter = ISecuritySchema(portal)
        security_adapter.set_enable_user_folders(True)

        # Create a members folder.
        create(Builder('folder').with_id('Members'))

        login(portal, TEST_USER_NAME)
        transaction.commit()

        # Login in the browser for the home folder to be created.
        browser = Browser(portal)
        browser.open(portal.absolute_url() + '/login_form')
        browser.getControl(name='__ac_name').value = TEST_USER_NAME
        browser.getControl(name='__ac_password').value = TEST_USER_PASSWORD
        browser.getControl(name='submit').click()

        # There must be a home folder inside the members folder now.
        membership_tool = getToolByName(portal, 'portal_membership')
        members_folder = membership_tool.getMembersFolder()
        self.assertTrue(members_folder.hasObject('test_user_1_'))

        # Create content in the user's home folder. This content must not be
        # present in the portlet.
        home_folder = membership_tool.getHomeFolder()
        create(Builder('document').within(home_folder))

        # Create more content which must be present in the portlet.
        folder = create(Builder('folder'))
        create(Builder('document').within(folder))

        # Render the portlet.
        portlet_renderer = self.renderer()
        portlet_content = [brain.portal_type for brain
                           in portlet_renderer._data()]

        self.assertEqual(
            sorted(portlet_content),
            sorted([u'Document', u'Folder']))

    def test_home_folder_contents_are_listed_in_portlet_if_enabled(self):
        portal = self.layer['portal']

        registry = getUtility(IRegistry)
        registry['ftw.dashboard.portlets.recentlymodified.'
                 'exclude_members_folder'] = False

        # Enable user folders.
        security_adapter = ISecuritySchema(portal)
        security_adapter.set_enable_user_folders(True)

        # Create a members folder.
        create(Builder('folder').with_id('Members'))

        login(portal, TEST_USER_NAME)
        transaction.commit()

        # Login in the browser for the home folder to be created.
        browser = Browser(portal)
        browser.open(portal.absolute_url() + '/login_form')
        browser.getControl(name='__ac_name').value = TEST_USER_NAME
        browser.getControl(name='__ac_password').value = TEST_USER_PASSWORD
        browser.getControl(name='submit').click()

        # There must be a home folder inside the members folder now.
        membership_tool = getToolByName(portal, 'portal_membership')
        members_folder = membership_tool.getMembersFolder()
        self.assertTrue(members_folder.hasObject('test_user_1_'))

        # Create content in the user's home folder. This content must be
        # present in the portlet.
        home_folder = membership_tool.getHomeFolder()
        create(Builder('document').within(home_folder))

        # Create more content which must be present in the portlet.
        folder = create(Builder('folder'))
        create(Builder('document').within(folder))

        # Get the portlet's content.
        portlet_renderer = self.renderer()
        portlet_content = [brain.getPath() for brain
                           in portlet_renderer._data()]

        self.assertEqual(
            sorted(portlet_content),
            sorted([
                '/plone/folder',
                '/plone/folder/document',
                '/plone/Members',
                '/plone/Members/test_user_1_',
                '/plone/Members/test_user_1_/document',
            ]))