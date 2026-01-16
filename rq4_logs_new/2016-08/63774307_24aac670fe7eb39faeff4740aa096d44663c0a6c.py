

class Component(object):
    """
    Component Hooks. Outlines build sequence for components, grabbing required data first from the guide
    inside __init__
    """
    def __init__(self, builder, guide):

        self.builder = builder
        self.guide = guide
        self.settings = self.guide.settings
        self.model = self.builder.model

        self.hiddenGrp = self.builder.hidden_grp
        self.unselectableGrp = self.builder.unselactableGrp
        self.controllersGrp = self.builders.controllersGrp
        self.deformersGrp = self.builder.deformersGrp

        self.deformersOrg = self.builder.deformersOrg

        self.name = self.guide.name
        self.location = self.guide.location

        # the five stages of a components build process. Like rigHooks I guess, createsObjects
        # for ALL components, then createsParameters for ALL components etc etc
        self.build = [self.createObjects, self.createParameters, self.createOperators,
                      self.createSlots, self.createConnection]

    def createObjects(self):
        pass

    def createParameters(self):
        pass

    def createOperators(self):
        pass

    def createSlots(self):
        pass

    def createConnection(self):
        pass