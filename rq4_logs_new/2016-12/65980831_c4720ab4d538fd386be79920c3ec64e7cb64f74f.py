from django.core.urlresolvers import reverse
from django.test import TestCase
from django.test.client import Client

from navbuilder import models
from navbuilder.tests.test_base import load_fixtures, load_crumb_fixtures

from django.template import Context, Template

crumb_template_1 = Template(
        "{% load navbuilder_tags %}"
        "{% navbuilder_breadcrumbs 'menu-1' %}"
        )

crumb_template_2 = Template(
        "{% load navbuilder_tags %}"
        "{% navbuilder_breadcrumbs 'menu-2' %}"
        )

crumb_template_3 = Template(
        "{% load navbuilder_tags %}"
        "{% navbuilder_breadcrumbs 'menu-3' %}"
        )


class BreadcrumbsTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        load_fixtures(self)
        load_crumb_fixtures(self)
        # Reorganise the items
        self.menuitem.link = None
        self.sub_menuitem_2.link = self.link


    def test_breadcrumbs(self):
        # The link object maps to the submenu in menu 1
        out = crumb_template_1.render(Context({"object": self.link}))
        self.assertHTMLEqual(out, """
                <a class="Crumb" href="/link/1/"
                title="Link 1" target="" data-slug="link-1">
                    Link 1
                </a>
                >
                <a class="Crumb" data-slug="link-1" href="/link/1/"
                target="blank" title="Link 1">
                    Link 1
                </a>
                """

        # The link object maps to a single level in menu 2
        out = crumb_template_2.render(Context({"object": self.link}))
        self.assertHTMLEqual(out, """
                <a class="Crumb" href="/link/1/" title="Link 1"
                target="" data-slug="link-1">
                    Link 1
                </a>
                """

        # If we cannot identify the menu it comes from, take the first one.
        out = crumb_template_3.render(Context({"object": self.link}))
        self.assertIn("Link 1", out)

    def tearDown(self):
        pass