""" Upgrades for eea.relations 7.7
"""

import logging
from persistent.list import PersistentList
from Products.CMFCore.utils import getToolByName

logger = logging.getLogger("eea.relations.upgrades")

def add_eea_refs(context):
    """
    Add the eea_refs attribute to objects
    if the object has related items, put them in eea_refs
    """
    ctool = getToolByName(context, 'portal_catalog')
    brains = ctool()
    logger.info("Total of %s objects" %len(brains))
    i = 0
    for brain in brains:
        i += 1
        if i%1000 == 0:
            logger.info("%s objects updated" %i)
        try:
            obj = brain.getObject()
            try:
                obj.eea_refs = PersistentList(obj.getRawRelatedItems())
            except:
                obj.eea_refs = PersistentList()
            obj._p_changed = 1
        except:
            logger.warn("brain with problems: %s" %brain.getPath())
    logger.info("Done adding eea_refs on objects")