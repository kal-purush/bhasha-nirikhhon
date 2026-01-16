class Crop:
    def __init__(self, x, y, w, h, name="crop"):
        self.x, self.y, self.w, self.h, self.name = x, y, w, h, name
    
    def getCrop(self):
        return self.x, self.y, self.w, self.h