from prac_08.taxi import Taxi
from prac_08.unreliable_car import UnreliableCar


def main():
    uber = Taxi('Prius 1', 100)
    uber.drive(40)
    print(uber)
    print("${}".format(uber.get_fare()))
    uber.start_fare()
    uber.drive(100)
    print(uber)
    print("${}".format(uber.get_fare()))


if __name__ == '__main__':
    main()