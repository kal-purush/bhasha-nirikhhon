class parking_lot:

  def __init__(self, motorcycle_spot_num, compact_spot_num, large_spot_num):
    self.motorcycle_spot_num = 30
    self.compact_spot_num = 50
    self.large_spot_num = 60
  
  def motor_spot_available(self):
    if self.motorcycle_spot_num > 0:
      return True
    else:
      return False

  def compact_spot_available(self):
    if self.compact_spot_num > 0: 
      return True
    else:
      return False
  
  def large_spot_available(self):
    if self.large_spot_num > 0: 
      return True
    else:
      return False

  def bus_spot_available(self):
    if self.large_spot_num >= 3:
      return True
    else:
      return False

  def park(vihecle):
    if vihecle.type == 'mortorcycle':
      if self.motor_spot_available():
        self.motorcycle_spot_num -= 1
      elif self.compact_spot_available():
        self.compact_spot_num -= 1
      elif self.large_spot_available():
        self.large_spot_num -= 1
      else:
        print('No more spot available')
        return False
      return True

      
    elif vihecle.type == 'car':
      if self.compact_spot_available():
        self.compact_spot_num -= 1
      elif self.large_spot_available():
        self.large_spot_num -= 1
      else:
        print('No more spot available')
        return False
    
    else:
      if self.bus_spot_available():
        return True
      else:
        return False
      
  def leave(vihecle):
    pass

class vihecle:
  def __init__(self, length, width):
    self.length = length
    self.width = width
    self.type = type

class mortorcycle(vihecle):
  def __init(self, length, width):
    vihecle.__init__(length, width)
    self.type = 'mortorcycle'
    self.type_of_spot = ''
  
  def set_spot(self,spot_type):
    self.type_of_spot = spot_type
    

class car(vihecle):
  def __init__(self, length, width):
    vihecle.__init__(4, length, width, 'car')
    

class bus(vihecle):
  def __init__(self, length, width):
    vihecle.__init__(4, length, width, 'bus')

m = mortorcycle(2,4)
print(m.type)
m.set_spot('motor')
print(m.type_of_spot)