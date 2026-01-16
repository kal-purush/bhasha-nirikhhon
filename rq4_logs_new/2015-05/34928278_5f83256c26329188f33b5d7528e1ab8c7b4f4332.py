class KnownAgent:
    def __init__(self, x, y, p_m, t_q, color, width, height):
        self.p_m = p_m
        self.t_q = t_q
        self.color = color
        self.width = width
        self.height = height
        self.particles = []
        self.particles.append((x, y))

    def move(self, motion):
        particles = []
        for particle in self.particles:
            particles.append(((particle[0] + motion[0])%self.width,(particle[1] + motion[1])%self.height))
        self.particles = particles

    def sense(self, transmissions):
        return