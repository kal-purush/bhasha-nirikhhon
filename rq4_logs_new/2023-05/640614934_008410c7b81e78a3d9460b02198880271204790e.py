"""
    DroneManager class with list drones
"""


class SetManager:
    """
         DroneManager class with list drones
    """
    def __init__(self, drone_manager):
        self.drone_manager = drone_manager
        self.index = 0

    def __len__(self):
        return sum(len(drone.best_cargo) for drone in self.drone_manager)

    def __iter__(self):
        for drone in self.drone_manager.drones:
            yield from drone.best_cargo

    def __getitem__(self, index):
        if index >= len(self):
            raise IndexError("Index out of range")

        for drone in self.drone_manager.drones:
            if index < len(drone.best_cargo):
                return drone.best_cargo[index]
            index -= len(drone.best_cargo)

    def __next__(self):
        if self.index >= len(self.drone_manager):
            raise StopIteration
        item = self.drone_manager.drones(self.index)
        self.index += 1
        return item