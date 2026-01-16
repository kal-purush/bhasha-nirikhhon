"""
    packages models
"""
from models.delivery_drone import DeliveryDrone
from models.petrol_drone import PetrolDrone
from models.electric_drone import ElectricDrone
from models.underwater_drone import UnderwaterDrone


class DroneManager:
    """
        DroneManager class with list drones
    """

    def __init__(self):
        self.__drones = []

    def add_drone(self, input_drone):
        self.__drones.append(input_drone)

    def find_all_with_speed_greater_than(self, speed):
        return list(filter(lambda needle_drone: needle_drone.current_speed > speed, self.__drones))

    def find_all_with_altitude_greater_than(self, altitude):
        return list(filter(lambda needle_drone: needle_drone.current_altitude > altitude, self.__drones))

    def print_drones(self):
        for index in self.__drones:
            index.calculate_max_flying_distance_at_current_speed()
            print(index)


if __name__ == '__main__':
    manager = DroneManager()

    drones = [DeliveryDrone(10, 50, 50, 50, 60),
              DeliveryDrone(50, 100, 600, 800, 100),
              PetrolDrone(15, 50, 30, 70, 90),
              PetrolDrone(100, 200, 300, 400, 500),
              ElectricDrone(300, 400, 500, 100, 200),
              ElectricDrone(150, 178, 459, 493, 584),
              UnderwaterDrone(334, 345, 567, 678, 567),
              UnderwaterDrone(346, 132, 136, 678, 355), ]

    for drone in drones:
        manager.add_drone(drone)

    manager.print_drones()

    print("\nDrones with speed greater than 100:\n")
    list_drones_with_speed_greater_than = manager.find_all_with_speed_greater_than(100)

    for drone in list_drones_with_speed_greater_than:
        drone.calculate_max_flying_distance_at_current_speed()
        print(drone)