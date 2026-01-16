from domain.reservation import Reservation

from utils.helper import Helper


class HotelController:

    def __init__(self, hotel_repository):
        self.__hotel_repository = hotel_repository

    def create_reservation(self, family_name, room_type, number_of_guests, arrival_date, departure_date):
        reservations = self.__hotel_repository.get_reservations()
        uid = Helper.generate_uid(reservations)
        available_rooms = self.__hotel_repository.get_available_rooms(arrival_date, departure_date, room_type)
        if len(available_rooms) == 0:
            raise Exception("No room available for the desired type and dates.")
        reservation = Reservation(uid, available_rooms[0].get_number(), family_name, number_of_guests, arrival_date, departure_date)
        HotelController.validate_reservation(reservation)
        self.__hotel_repository.add_reservation(reservation)

    def delete_reservation(self, uid):
        self.__hotel_repository.remove_reservation(uid)

    def retrieve_available_rooms(self, start_date, end_date):
        return self.__hotel_repository.get_available_rooms(start_date, end_date)

    def retrieve_monthly_report(self):
        return self.__hotel_repository.get_monthly_report()

    def retrieve_days_of_week_report(self):
        return self.__hotel_repository.get_days_of_week_report()

    @staticmethod
    def validate_reservation(reservation):
        if len(reservation.get_family_name()) == 0:
            raise ValueError("No family name.")
        if reservation.get_arrival_date() >= reservation.get_departure_date():
            raise ValueError("Invalid arrival/departure dates.")
        if reservation.get_number_of_guests() < 1 or reservation.get_number_of_guests() > 4:
            raise ValueError("There cannot be less than 1 or more than 4 guests.")