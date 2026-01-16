
# -*- coding: utf-8 -*-
from kitconcept.seo import _
from plone.autoform.interfaces import IFormFieldProvider
from plone.dexterity.interfaces import IDexterityContent
from plone.namedfile import field as namedfile
from plone.supermodel import model
from zope import schema
from zope.component import adapter
from zope.interface import implementer
from zope.interface import provider


@provider(IFormFieldProvider)
class ISeo(model.Schema):

    model.fieldset(
        'seo', 
        label=_(u'SEO'),
        fields=[
            'seo_title', 
            'seo_description'
        ]
    )

    seo_title = schema.TextLine(
        title=_("Title"),
        description=_(
            u"Override the meta title. When empty the default title will be used. " +
            u"Use maximum 50 chararcters."
        ),
        required=False,
        max_length=70,
    )

    seo_description = schema.TextLine(
        title=_(u"Description"),
        description=_(
            u"Override the meta description. When empty " +
            u"the default description will be used. Use maximum 150 characters."),
        required=False,
        max_length=155,
    )


@implementer(ISeo)
@adapter(IDexterityContent)
class Seo(object):

    def __init__(self, context):
        self.context = context