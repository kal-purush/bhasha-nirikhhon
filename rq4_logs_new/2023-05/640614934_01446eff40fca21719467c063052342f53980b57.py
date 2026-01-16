class Drone:
    instance = None

    def __init__(self, current_speed=0, current_altitude=0, battery_capacity=0, current_battery_level=0):
        self.current_speed = current_speed
        self.current_altitude = current_altitude
        self.battery_capacity = battery_capacity
        self.current_battery_level = current_battery_level

    def fly_at(self, speed_in_meters_per_second, altitude_in_meters):
        self.current_speed = speed_in_meters_per_second
        self.current_altitude = altitude_in_meters

    def use_battery(self, amount):
        self.current_battery_level -= amount

    def charge_battery(self, amount):
        self.current_battery_level += amount

    @staticmethod
    def get_instance():
        if not Drone.instance:
            Drone.instance = Drone()
        return Drone.instance

    def __str__(self):
        return f"current_speed={self.current_speed}, current_altitude={self.current_altitude}, " \
               f"battery_capacity={self.battery_capacity}, current_battery_level={self.current_battery_level}"


if __name__ == '__main__':
    drones = [Drone(100, 200, 1000, 2000), Drone(), Drone.get_instance(), Drone.get_instance()]

    for drone in drones:
        print(drone)