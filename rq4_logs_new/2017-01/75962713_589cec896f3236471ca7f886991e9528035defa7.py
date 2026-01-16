# coding: utf-8

from scene import *
from main_menu import *
import sound
import random
import math


class MainMenuScene (Scene):
	def setup(self):
		self.background = SpriteNode('./assets/sprites/background2.JPG', size = self.size, position = self.size/2, parent = self)
	
		self.button = SpriteNode('./assets/sprites/button3.PNG', size = self.size/5, position = self.size/2, parent = self)
	def did_change_size(self):
		pass
	
	def update(self):
		pass
	
	def touch_began(self, touch):
		if self.button.frame.contains_point(touch.location):
			self.present_modal_scene(MainMenuClass())
	
	def touch_moved(self, touch):
		pass
	
	def touch_ended(self, touch):
		pass