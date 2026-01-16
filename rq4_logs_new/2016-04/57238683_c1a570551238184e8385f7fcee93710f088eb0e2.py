#!/usr/bin/python

import math
import numpy
from numpy import linalg, array

WIDTH, HEIGHT = 200, 200

def normalized(x):
	return x / linalg.norm(x)

class Ray:
	def __init__(self, origin, direction):
		self.origin, self.direction = origin, normalized(direction)

class Hit:
	def __init__(self, point, incoming, normal, reflection, travel):
		self.point, self.incoming, self.normal, self.reflection, self.travel = point, incoming, normal, reflection, travel

class Light:
	def __init__(self, point, color):
		self.point, self.color = point, color

class Material:
	def __init__(self):
		pass

class Sphere:
	def __init__(self, material, center, radius):
		self.material, self.center, self.radius = material, center, radius

	def cast_test(self, ray):
		offset = self.center - ray.origin
		base_d = ray.direction.dot(offset)
		discriminant = base_d**2 - linalg.norm(offset)**2 + self.radius**2
		if discriminant < 0:
			return
		root = math.sqrt(discriminant)
		if base_d - root < 0:
			return
		hit = ray.origin + ray.direction * (base_d - root - 1e-2)
		normal = normalized(hit - self.center)
		reflection = normalized(ray.direction - (2 * normal.dot(ray.direction)) * normal)
		return Hit(hit, ray.direction, normal, reflection, base_d - root)

class Plane:
	def __init__(self, material, normal, height):
		self.material, self.normal, self.height = material, normal, height

	def cast_test(self, ray):
		origin_height = self.normal.dot(ray.origin) - self.height
		if origin_height < 0:
			return
		rate = -self.normal.dot(ray.direction)
		if rate <= 0:
			return
		approach_distance = origin_height / rate
		hit = ray.origin + ray.direction * approach_distance
		reflection = normalized(ray.direction - (2 * self.normal.dot(ray.direction)) * self.normal)
		return Hit(hit, ray.direction, self.normal, reflection, approach_distance)

class Scene:
	def __init__(self):
		self.objects = []
		self.lights = []

	def cast_test(self, ray):
		best_travel = float("inf")
		best_hit = None
		for obj in self.objects:
			hit = obj.cast_test(ray)
			if hit and hit.travel < best_travel:
				best_travel = hit.travel
				best_hit = hit
		return best_hit

	def color_ray(self, ray, recursions=0):
		energy = array([0.0, 0.0, 0.0])
		hit = self.cast_test(ray)
		if not hit:
			return energy
		# Cast shadow rays to each light.
		for light in self.lights:
			to_light = light.point - hit.point
			shadow_ray = Ray(hit.point, to_light)
			shadow_hit = self.cast_test(shadow_ray)
			distance_to_light = linalg.norm(to_light)
			if (not shadow_hit) or shadow_hit.travel > distance_to_light:
				raw_energy = light.color / distance_to_light**2.0
				lambertian_coef = hit.normal.dot(shadow_ray.direction)
				phong_coef = max(0.0, hit.reflection.dot(shadow_ray.direction))**15.0
				energy += raw_energy * (lambertian_coef + phong_coef)
		# If we have recursions left do an idea reflection.
		return energy

	def render(self, surface):
		aspect_ratio = WIDTH / HEIGHT
		camera_origin = array([0.0, -5.0, 1.0])
		plane_height = 0.8
		for y in xrange(HEIGHT):
			for x in xrange(WIDTH):
				ray_direction = array([aspect_ratio * ((x - WIDTH/2.0)/HEIGHT) * plane_height, 1, ((HEIGHT/2.0 - y)/HEIGHT) * plane_height])
				ray = Ray(camera_origin, ray_direction)
				energy = self.color_ray(ray, 2)
				color = self.energy_to_color(energy)
				surface.set_at((x, y), color)

	def energy_to_color(self, energy):
		f = lambda x: int(max(0.0, min(255.0, x*255.0)))
		return map(f, energy)

scene = Scene()
ground_mat = Material()
scene.objects.append(Sphere(ground_mat, array([0.0, 0.0, 1.0]), 0.8))
scene.objects.append(Plane(ground_mat, array([0.0, 0.0, 1.0]), 0.0))
scene.lights.append(Light(array([-2.0, -2.0, 3.0]), 5 * array([1.0, 1.0, 1.0])))

import pygame, time
pygame.init()
screen = pygame.display.set_mode((WIDTH, HEIGHT))
start = time.time()
scene.render(screen)
stop = time.time()
print "Rendered in: %.3f" % (stop - start)
while True:
	for event in pygame.event.get():
		if event.type == pygame.QUIT or (event.type == pygame.KEYDOWN and event.key == 27):
			pygame.quit()
			exit()
	pygame.display.update()
	time.sleep(0.01)
