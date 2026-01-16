from main_menu import *
from scene import *
from star import *
import sound
import random
import math
import json
import os
from save_system import *
from time import sleep
from objc_util import *
import threading

#main_view = splash_screen.main_view
class GameScene (Scene):
	
	def setup(self):
		#self.tutorial = json.load(joined)
		#self.tutorial = open('saved_data.json','r+')
		#self.saved_data = 
		#json.dump({"tutorial":"true"},self.tutorial)
		#self.tutorial.close()
		#print(self.saved_data['tutorial'])
		#file.write(joined)
		
		self.total_hydrogen = 0
		self.hydrogen_deplete = 0
		self.pressed = False
		self.close = False
		self.enable_saving = save()
		self.load = self.enable_saving.loading()
		print(self.load)
		self.newstar = star_class(self.load['hydrogen_amount'])
		self.newstar.data(self.load['tier'])
		self.newstar.hydrogen_deplete()
		self.enable_saving.data(self.newstar, self.load['tier'])
		self.enable_saving.saving()
		self.old_hydrogen = self.newstar.get_hydrogen()
		
		self.sideframe = SpriteNode('./assets/sprites/frame.PNG', size = (self.size[0]/3.5, self.size[1]), alpha = 0.2)
		
		self.line = SpriteNode('./assets/sprites/line.PNG',parent = self ,size = (self.size[0]/(self.size[0]/2),self.size[1]/(self.size[1]/2)), position = (self.size[0]/3,self.size[1]/3), z_position = 9.0,alpha=0)
		
		self.line2 = SpriteNode('./assets/sprites/line.PNG',parent = self ,size = (self.size[0]/(self.size[0]/2),self.size[1]/(self.size[1]/2)), position = (self.size[0]/2.67,self.size[1]/7), z_position = 9.0,alpha=0)
		
		self.line3 = SpriteNode('./assets/sprites/line.PNG',parent = self ,size = (self.size[0]/(self.size[0]/2),self.size[1]/(self.size[1]/2)), position = (self.size[0]/5.75,self.size[1]/2.50), z_position = 9.0, alpha=0)
		
		self.button_one = SpriteNode('./assets/sprites/statistics_button.PNG',parent=self,size = (self.size[0]/8.5, self.size[0]/8.5), position  = (self.size[0]/1.9, self.size[1]/1.75), z_position = 5.0, alpha = 0)
		
		self.line.rotation = math.radians(315.0)
		self.line2.rotation = math.radians(300.0)
		self.line3.rotation = math.radians(330.0)
		
		self.line.anchor_point = (0,0)
		self.line2.anchor_point = (0,0)
		self.line3.anchor_point = (0,0)
		
		#self.button_one = SpriteNode('./assets/sprites/button3.PNG',parent=self,size = (250,60), position = (self.size[0]/2.00,self.size[1]/2+15), z_position = 5.0, alpha = 0)
		
		#self.button_two = SpriteNode('./assets/sprites/button3.PNG',parent=self,size = (250,60), position = (self.size[0]/1.84,self.size[1]/2.75+15), z_position = 5.0, alpha = 0)
		
		#self.button_three = SpriteNode('./assets/sprites/button3.PNG',parent=self,size = (250,60), position = (self.size[0]/1.85,self.size[1]/4.5+15), z_position = 5.0, alpha = 0)
		self.pause_button = SpriteNode('./assets/sprites/pause_button.PNG', parent=self, size = (self.size[0]/10.24,self.size[1]/7.68), position = (self.size[0]/10,self.size[1]),z_position=5.0)
		self.pause_button.position = (0 + self.pause_button.size.x/2, self.size[1] - self.pause_button.size.y/2)
		self.button = SpriteNode('./assets/sprites/fire.PNG', parent=self, size = (self.size[0]/4.2314,self.size[1]/3.173), position = (self.size[0]/4.96,self.size[1]/4.93) , z_position = 2, alpha=0.01)
		
		self.settings_node = Node(parent=self, position = (-self.sideframe.size[0]/2, self.size[1]/2), z_position = 6.0)
		
		self.settings_node.add_child(self.sideframe)
		
		SpriteNode('./assets/sprites/IMG_2640.PNG', size = self.size, position=self.size/2, parent=self, z_position = 1)
		
		#hydrogen = list(str(self.newstar.get_hydrogen()/10**27))
		
		#hydrogen.insert(1, ".")
		
		#total_hydrogen = ''
		
		#for i in hydrogen:
			
		#	total_hydrogen = total_hydrogen + i
		print(str(self.newstar.get_hydrogen()) + 'stuff')
		self.hydrogen = LabelNode(parent=self.settings_node, text = 'Hydrogen = ' + self.hydrogen_size_check(self.newstar.get_hydrogen())[1] + self.hydrogen_size_check(self.newstar.get_hydrogen())[0] )
		
		self.hydrogen.size = (self.sideframe.size[0], self.sideframe.size[1]/20)
		
		with open('./assets/shaders/SimplexNoise.fsh') as f:
			src = f.read()
			shader = Shader(src)
			
		self.sprite = SpriteNode('./assets/sprites/SimplexNoiseGradient.PNG', size=self.size, position=self.size/2, parent=self, z_position = 0)
		
		self.sprite.shader = shader
		
	def did_change_size(self):
		pass
	
	def update(self):
		
		if self.old_hydrogen != self.newstar.get_hydrogen():
			
			self.hydrogen.text = ('Hydrogen = ' +  self.hydrogen_size_check(self.newstar.get_hydrogen())[1] + self.hydrogen_size_check(self.newstar.get_hydrogen())[0])
			
			self.old_hydrogen = self.newstar.get_hydrogen()
		
	def touch_began(self, touch):
		
		if self.button_one.frame.contains_point(touch.location) and self.button_one.alpha == 0.8:
			
			if self.settings_node.position[0] == self.sideframe.size[0]/2:
				
				self.settings_node.run_action(Action.move_to(-self.sideframe.size[0]/2, self.size[1]/2, 0.5))
			elif self.settings_node.position[0] == -self.sideframe.size[0]/2:
				
				self.settings_node.run_action(Action.move_to(self.sideframe.size[0]/2, self.size[1]/2, 0.5))
				self.close = True
				
		if self.button.frame.contains_point(touch.location) or self.close == True:
				if self.pressed == False:
					
					#self.ui_curve.run_action(Action.fade_to(1, 1))
					#self.button_three.run_action(Action.fade_to(1, 1))
					#self.button_two.run_action(Action.fade_to(1, 1))
					self.line.alpha = 1
					self.line2.alpha = 1
					self.line3.alpha = 1
					self.line.run_action((Action.scale_y_to(self.size[0]/10, 1)))
					self.line2.run_action((Action.scale_y_to(self.size[0]/10, 1)))
					self.line3.run_action((Action.scale_y_to(self.size[0]/10, 1)))
					self.button_one.run_action(Action.fade_to(0.8, 1))
					self.pressed = True
				else:
					self.line.run_action((Action.scale_y_to(self.size[0]/(self.size[0]/2), 1)))
					self.line2.run_action((Action.scale_y_to(self.size[0]/(self.size[0]/2), 1)))
					self.line3.run_action((Action.scale_y_to(self.size[0]/(self.size[0]/2), 1)))
					self.line.run_action(Action.fade_to(0, 2))
					self.line2.run_action(Action.fade_to(0, 2))
					self.line3.run_action(Action.fade_to(0, 2))
					#self.button_three.run_action(Action.fade_to(0, 1))
					#self.button_two.run_action(Action.fade_to(0, 1))
					self.button_one.run_action(Action.fade_to(0, 1))
					self.pressed = False
					self.close = False
		if self.pause_button.frame.contains_point(touch.location):
			
			self.enable_saving.pause()
			
		if self.button_one.frame.contains_point(touch.location):
			#self.dismiss_modal_scene()
			pass

	def touch_moved(self, touch):
		pass
	
	def touch_ended(self, touch):
		pass
	def pause(self):
		self.enable_saving.pause()
	def hydrogen_size_check(self, hydrogen):
		
		if hydrogen >= 1000000000000000000 and hydrogen < 1000000000000000000000:
			
			return ('quintillion',self.decimal_inputer(self.newstar.get_hydrogen(),16))
			
		if hydrogen >= 1000000000000000000000 and hydrogen < 1000000000000000000000000:
			
			return ('sextillion',self.decimal_inputer(self.newstar.get_hydrogen(),19))
			
		if hydrogen >= 1000000000000000000000000 and hydrogen < 1000000000000000000000000000:
			
			return('septillion',self.decimal_inputer(self.newstar.get_hydrogen(),22))
			
		if hydrogen >= 1000000000000000000000000000 and hydrogen < 1000000000000000000000000000000:
			
			return ('octillion',self.decimal_inputer(self.newstar.get_hydrogen(),25))
			
		if hydrogen >= 1000000000000000000000000000000 and hydrogen < 1000000000000000000000000000000000:
			
			return ('nonillion',self.decimal_inputer(self.newstar.get_hydrogen(),28))
			
		if hydrogen >= 1000000000000000000000000000000000 and hydrogen < 1000000000000000000000000000000000000:
			
			return ('decillion',self.decimal_inputer(self.newstar.get_hydrogen(),31))
			
		if hydrogen >= 1000000000000000000000000000000000000:
		
			return ('undecillion',self.decimal_inputer(self.newstar.get_hydrogen(),34))
			
	def decimal_inputer(self, number, size):
		new_amount = ''
		hydrogen_amount = list(str(number/10**size))
		hydrogen_amount.insert(len(hydrogen_amount) -2, ".")
		
		for characters in hydrogen_amount:
			new_amount = new_amount + characters
		return new_amount