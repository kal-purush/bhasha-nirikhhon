from models import Customer, Vehicle, Rental
from dao import CustomerDAO, VehicleDAO, RentalDAO
import datetime

def main():
    cdao = CustomerDAO()
    vdao = VehicleDAO()
    rdao = RentalDAO()

    while True:
        print("\n--- Vehicle Rental System ---")
        print("1. Add Customer")
        print("2. Add Vehicle")
        print("3. Rent Vehicle")
        print("4. Exit")
        choice = input("Choose an option: ")

        if choice == '1':
            name = input("Enter customer name: ")
            email = input("Enter customer email: ")
            c = Customer(name, email)
            cdao.add_customer(c)
            print("Customer added.")

        elif choice == '2':
            model = input("Enter vehicle model: ")
            v = Vehicle(model)
            vdao.add_vehicle(v)
            print("Vehicle added.")

        elif choice == '3':
            cid = input("Enter customer ID: ")
            vid = input("Enter vehicle ID: ")
            date = datetime.datetime.now().strftime("%Y-%m-%d")
            r = Rental(cid, vid, date)
            rdao.rent_vehicle(r)
            print("Vehicle rented.")

        elif choice == '4':
            break

        else:
            print("Invalid choice.")

if __name__ == '__main__':
    main()