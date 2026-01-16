from cmivfx import xsi, log
from datetime import datetime


"""
Builder takes guide model and create a 'rig' model from the parameters specified in the guide.
Rig model contains all rig elements - components, groups, geometry etc.
Builder needs to be instantiated with instance of guide module.
"""
class Builder(object):

    def __init__(self, guide):
        self.guide = guide
        self.settings = self.guide.settings

    def build(self):
        """Master call for building rig elements. Logs completion time for convenience."""
        startTime = datetime.now()

        xsi.SetValue('preferences.scripting.cmdlog', False, '')
        self.buildInitialHierarchy()
        xsi.SetValue('preferences.scripting.cmdlog', True, '')

        endTime = datetime.now()
        log('Build completed in: {}'.format(endTime - startTime))

    def buildInitialHierarchy(self):
        """"Creates default rig model hierarchy for organising rig components."""
        # Rig model (different from guide model)
        self.model = xsi.ActiveSceneRoot.AddModel(None, self.settings['Name'])
        self.model.Properties('Visibility').Parameters('viewvis').Value = False

        # Groups for organisation
        self.hiddenGrp = self.model.AddGroup(None, 'hidden_grp')
        self.unselactableGrp = self.model.AddGroup(None, 'unselactable_grp')
        self.controllersGrp = self.model.AddGroup(None, 'controllers_grp')
        self.deformersGrp = self.model.AddGroup(None, 'deformers_grp')

        self.hiddenGrp.Parameters('viewvis').Value = 0
        self.unselactableGrp.Parameters('selectability').Value = 0
        self.deformersGrp.Parameters('viewvis').Value = 0

        # Nulls for organisation
        self.deformersOrg = self.model.AddNull('deformers_org')
        self.geometryOrg = self.model.AddNull('geometry_org')
        self.hiddenGrp.AddMember(self.deformersOrg)
        self.hiddenGrp.AddMember(self.geometryOrg)
