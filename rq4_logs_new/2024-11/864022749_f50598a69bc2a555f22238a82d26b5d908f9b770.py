from unreliable_car import UnreliableCar


def main():
    """Test the UnreliableCar class"""

    reliable_car = UnreliableCar("Mostly Reliable", 100, 90)
    unreliable_car = UnreliableCar("Mostly Unreliable", 100, 10)

    for i in range(1, 6):
        print(f"Attempt {i}:")
        print(f"{reliable_car.name} drove {reliable_car.drive(20)} km.")
        print(f"{unreliable_car.name} drove {unreliable_car.drive(20)} km.\n")

    print("Final states:")
    print(reliable_car)
    print(unreliable_car)


main()