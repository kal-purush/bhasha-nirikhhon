from models.drone import AbstractDrone


class DeliveryDrone(AbstractDrone):
    """
        Represents a drone with speed, altitude, battery capacity, and battery level attributes.
    """

    def __init__(self, current_speed,
                 current_altitude,
                 battery_capacity,
                 current_battery_level,
                 consumption_battery, ):
        """
            Initializes a new instance of the Drone class.
        """
        super().__init__(current_speed, current_altitude)
        self.battery_capacity = battery_capacity
        self.current_battery_level = current_battery_level
        self.consumption_battery = consumption_battery
        self.current_max_flying_distance = None

    def use_battery(self, amount):
        """
            Reduces the current battery level of the drone by the specified amount.
        """
        self.current_battery_level -= amount

    def charge_battery(self, amount):
        """
            Increases the current battery level of the drone by the specified amount.
        """
        self.current_battery_level += amount

    def calculate_max_flying_distance_at_current_speed(self):
        """
            Calculate max flying distance with current speed
        """
        self.current_max_flying_distance = (self.current_battery_level / self.consumption_battery) * 100

    def __str__(self):
        """
            Returns a string representation of the Drone object.
        """
        return f"{super().__str__()}" \
               f"battery_capacity={self.battery_capacity}, " \
               f"current_battery_level={self.current_battery_level}, " \
               f"consumption_battery={self.consumption_battery}, " \
               f"max_flying_distance={self.current_max_flying_distance}"