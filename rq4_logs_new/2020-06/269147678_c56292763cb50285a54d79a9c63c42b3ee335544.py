class Body:
  def __init__(self, name, bodyparts):
    self.name = name
    self.parts = bodyparts


class BodyPart:
  def __init__(self, name, partType, size, canWear, canWield):
    self.name = name
    self.partType = partType
    self.size = size
    self.canWear = canWear
    self.canWield = canWield
