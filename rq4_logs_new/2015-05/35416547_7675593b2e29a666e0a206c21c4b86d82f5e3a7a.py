import numpy as np
import scipy as sp

class Penguin(object):

	def __init__(self, radius, position, alignment, boundary):
		"""
		Arguments:
			radius		:=	float representing penguin radius
			position	:=	numpy array representing penguin position (x, y, z)
			alignment	:=	numpy array representing alignment unit vector
			boundary	:=	boolean value, True if boundary penguin, False if bulk penguin
		"""

		self.radius = radius
		self.position = position
		self.alignment = alignment
		self.boundary = boundary

	def get_distance(self, penguin2):

		return self.position - penguin2.position

	def net_force(self, F_self, F_in, k, penguin_list, a):
		"""
		Arguments:
			F_self			:=	multiplicative strength factor of penguin self-propulsion
			F_in			:=	multiplicative strength factor of the penguin boundary force
			k				:=	spring constant relating to the repulsive force of overlapping penguins
			penguin_list	:=	python list containing all penguins in the system
			a 				:=	average radius of all penguins in the system
		"""

		critical_radius = 1.30 * a

		F_selfPropulsion = F_self * self.alignment

		F_repulsion = 0 

		if (self.boundary == True):

			F_boundary = F_in * self.alignment

		else:

			F_boundary = 0

		for i in range(len(penguin_list)):

			if (penguin_list[i] != self):

				r = self.get_distance(penguin_list[i])
				r_mag = np.linalg.norm(r)

				if (r < critical_radius):

					F_repulsion += -k * r

		return F_selfPropulsion + F_boundary + F_repulsion

	def net_torque(self, T_in, T_noise, T_align):

		








