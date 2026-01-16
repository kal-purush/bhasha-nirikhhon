#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  OEOLIS_parser.py
#  
#  Copyright 2015 220 <220@WKH>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  

import serial
import random
import pygame
import time

snap_delay = 150

width = 896
height = 256

# the only who's really judging you is yourself... nobody else.
def main():
	value = 0
	raw_val = 0
	active = True
	x = 0
	
	screen = pygame.display.set_mode ((width, height))
	pygame.display.set_caption ("OEOLIS")
	
	pygame.init ()
	pygame.time.set_timer (pygame.USEREVENT+1, snap_delay)
	
	comm = serial.Serial ("/dev/ttyACM0", 115200)
	
	c = (0, 127, 255)
	
	filename = time.strftime ("capture_%d-%m-%Y_%H:%M:%S.data")
	
	fstream = open (filename, 'w');
	while active:		
		if comm.inWaiting ()>0:
			raw_val = comm.read ()

		for event in pygame.event.get ():
			if event.type==pygame.USEREVENT+1:
				
				raw_val = str (raw_val)
				value = ord (raw_val)
				print value
				
				pygame.draw.line (screen, c, (x, 0), (x, value))
				
				fstream.write (str (value))
				fstream.write ("\n")
				
				x+= 1
				if x>width:
					x = 0
					#screen.fill ((0,0,0))
					c = (random.randrange (0, 255), random.randrange (0, 255), random.randrange (0, 255))
				
			elif event.type==pygame.KEYDOWN:
				if event.key==pygame.K_q:
					active = False
				elif event.key==pygame.K_r:
					screen.fill ((0,0,0))
					x = 0
				elif event.key==pygame.K_c:
					screen.fill ((0,0,0))
					
		

		pygame.display.update ()
	
	fstream.close ()
	
	pygame.quit ()
	comm.close ()
	
	return 0

if __name__ == '__main__':
	main()
