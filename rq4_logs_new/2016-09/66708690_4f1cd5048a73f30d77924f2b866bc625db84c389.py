import math

class Sphere(r = 1.0, x = 0.0, y = 0.0, z = 0.0):
    
    def get_volume(self, r):
        return (4/3*math.pi*(r**3))

    def get_square(self, r):
        return (4*math.pi*(r**2))

    def get_radius(self):
        return (self.r)
    
    def get_center(self):
        print ((x, y, z))

    def set_radius(self, r):
        self.r = r

    def set_center(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def is_point_inside(x,y,z):
        if -self.r <= x <= self.r and -self.r <= y <= self.r and -self.r <= z <= self.r :
            print (True)
        else:
            print (False)