import time
import scipy
import matplotlib
import matplotlib.pyplot as plt
import numpy
from scipy.optimize import curve_fit
class interfaceInformation():
  #Initialize with the clock system of the interface and robot
  def __init__(self, clock): 
    self.clock = clock
    self.start_time = clock.time.time() #There will be a little bit of lag since this function will be called after interface is called
   
  def speed_estimates(self, C_d, rau, frontal_area, andgle_of_attack):
    self.speed_left = math.sqrt((2*self.robot["left_motor"])/(C_d*rau*frontal_area*sin(angle_of_attack)))
    self.speed_right = math.sqrt((2*self.robot["right_motor"])/(C_d*rau*frontal_area*sin(angle_of_attack)))
    self.speed_up = math.sqrt((2*self.robot["up_motor"])/(C_d*rau*frontal_area*sin(angle_of_attack))
    
    magnitude = math.sqrt((self.speed_left)**2 + (self.speed_right)**2 + (self.speed_up)**2)
    return [magnitude, self.speed_left, self.speed_right, self.speed_up]
  
  
  def angular_position_arrays(self):
  
    time_start = self.clock.time.time()
    tdata = np.linspace(0,10,11)
    self.move_angle = self.robot["move_turn"]
    time_new = self.clock.time.time()
    velocity_data = []
    while len(velocity_data)<10 and time_new-time_start < 10:
      velocity_data.append(self.move_angle)
      time_new = self.clock.time.time()
    velocity_found = velocity_data
    velocity_data = []
    return [tdata, velocity_found]
    
  def angular_position_changer(time, angular_velocity):
  
    angular_position_change = 0
    for i in range(0, len(angular_velocity)-1):
      angular_position_change += (angular_velocity[i] + angular_velocity[i+1])/2
    return angular_position_change 