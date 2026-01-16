class Hotel:
    def __init__(self):
        self.rooms = {
            '1': {'guest': None, 'services': []},
            '2': {'guest': None, 'services': []},
            '3': {'guest': None, 'services': []},
            '4': {'guest': None, 'services': []},
            '5': {'guest': None, 'services': []},
            '6': {'guest': None, 'services': []},
            '7': {'guest': None, 'services': []},
            '8': {'guest': None, 'services': []},
            '9': {'guest': None, 'services': []},
            '10': {'guest': None, 'services': []},
            '11': {'guest': None, 'services': []},
            '12': {'guest': None, 'services': []},
            '13': {'guest': None, 'services': []},
            '14': {'guest': None, 'services': []},
        }
        self.available_services = ['Завтрак', 'Ужин', 'Массаж', 'Трансфер', 'Экскурсия', 'Фитнес']

    def display_available_rooms(self):
        available_rooms = [room_num for room_num, info in self.rooms.items() if info['guest'] is None]
        print("Доступные номера для бронирования:", available_rooms)

    def book_room(self):
        self.display_available_rooms()
        room_num = input("Введите номер комнаты для бронирования: ")
        guest_name = input("Введите ваше имя: ")
        if room_num in self.rooms and self.rooms[room_num]['guest'] is None:
            self.rooms[room_num]['guest'] = guest_name
            print(f"Номер {room_num} успешно забронирован на имя {guest_name}.")
        else:
            print("Извините, номер уже занят или не существует.")

    def provide_services(self):
        print("Доступные услуги: " + ', '.join(self.available_services))
        services_input = input("Введите услуги через запятую для вашего номера: ").split(',')
        services = [service.strip() for service in services_input if service.strip() in self.available_services]
        
        if not services:
            print("Ни одна из введенных услуг не доступна. Пожалуйста, введите корректные услуги.")
            return

        # Здесь предполагается, что номер комнаты уже известен, например, после бронирования
        room_num = self.get_room_number_for_services()
        if room_num and self.rooms[room_num]['guest'] is not None:
            self.rooms[room_num]['services'].extend(services)
            print(f"Услуги {services} успешно добавлены для номера {room_num}.")
        else:
            print("Номер не найден или не забронирован.")

    def get_room_number_for_services(self):
        # Этот метод можно использовать для получения номера комнаты от пользователя
        # после бронирования или для добавления услуг
        return input("Введите номер вашей комнаты: ")

    def guest_list(self):
        print("Список гостей и услуг:")
        for room_num, info in self.rooms.items():
            if info['guest'] is not None:
                print(f"Номер {room_num}: Гость {info['guest']}, Услуги: {info['services']}")

# Использование класса
hotel = Hotel()

# Пользователь может вызвать методы для бронирования номера и добавления услуг
hotel.book_room()
hotel.provide_services()

# Вывод списка гостей и услуг
hotel.guest_list()