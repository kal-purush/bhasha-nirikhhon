import sys
sys.path.insert(0,'C:\Users\gazi\Documents\OpenMDAO')
import openmdao

##importing openmdao classes
from openmdao.api import Group, Problem, Component, IndepVarComp

class Inlet_Condition(Component):
    def __init__(self):
	super(Inlet_Condition, self).__init__()
	 self.add_param('ambient_temp', 298.0, desc='Ambient Temp', units='K')
	 self.add_param('tube_pressure', 100.0, desc='Tube Pressure', units='Pa')
	 self.add_param('Density', 1.17E-03, desc='Density', units='kg/m^3')
	 self.add_param('R', 287.0, desc='Gas Constant', units='J/kg*K')
	 self.add_param('sound_speed', 298.0, desc='Ambient Temp', units='K')
	 self.add_param('inlet_area', 1008.0, desc='Inlet Area', units='m^2')
	 self.add_param('total_CR', 15.0, desc='Total CR', units='None')
	 
	 