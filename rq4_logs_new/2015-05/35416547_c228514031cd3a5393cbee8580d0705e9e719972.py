import scipy as sp
import numpy as np

def heaviside(number):

	if number < 0:
		return 0
	else:
		return 1

# def find_theta(vector1, vector2):

# 	v1_mag = np.linalg.norm(vector1)
# 	v2_mag = np.linalg.norm(vector2)

# 	return np.arccos(np.dot(vector1, vector2) / (v1_mag * v2_mag))

class Penguin(object):

	def __init__(self, radius, position, alignment):

		self.radius = radius
		self.position = position	# Use numpy array for position
		self.alignment = alignment

	def __str__(self):

		return "Penguin located at {}".format(self.position)

	def __repr__(self):

		return self.__str__()

	def get_distance(self, penguin2):

		return self.position - penguin2.position

	def net_force(self, F_self, F_in, k, penguin_list):

		crit_radius = 1.0

		F_sp = F_self * self.alignment

		theta_list = []
		net_repulse_distance = np.array([0,0,0])

		F_boundary = np.array([0,0,0])

		for i in range(len(penguin_list)):

			if (penguin_list[i] != self):

				r = self.get_distance(penguin_list[i])
				net_repulse_distance += r

				if np.linalg.norm(r) < crit_radius:

					theta = np.arctan2(r[1], r[0])
					theta_list.append(theta)

		theta_list.sort()

		for j in range(len(theta_list)):

			try:

				difference = theta_list[j+1] - theta_list[j]

			except IndexError:

				difference = (np.pi - theta_list[j]) + (theta_list[0] + np.pi)
			
			print(difference * 180 / np.pi)
			F_boundary += (difference - np.pi) * F_in * heaviside(difference - np.pi) * self.alignment

			print(F_boundary)

		F_repulsion = -k * net_repulse_distance

		return F_sp + F_boundary + F_repulsion

particle1 = Penguin(0.4, np.array([0,0,0]), np.array([1,1,1]))
particle2 = Penguin(0.4, np.array([0.25,0.25,0]), np.array([1,1,1]))
particle3 = Penguin(0.4, np.array([-0.25,0.25,0]), np.array([1,1,1]))

penguin_list = [particle1, particle2, particle3]

print(particle1.net_force(1,1,0,penguin_list))







