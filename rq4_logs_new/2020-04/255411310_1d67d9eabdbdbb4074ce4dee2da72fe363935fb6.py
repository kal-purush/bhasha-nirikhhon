class DiscountCard:
    def __init__(self, *, date="01/04/2020", amount=0, card_number):
        self.__card_number = card_number
        self.__discount = 1
        self.__amount = 0  # приховане поле

        if self.__check_date(date):
            self.__date = date
        else:
            self.__date = '01/04/2020'

        if amount != 0:
            self.__add_money(amount)

    # Перевірка чи можна конвертувати отримане значення в число (int)

    def __check_int(self, amount):
        try:
            int(amount)
        except ValueError:
            return False
        else:
            return True

    # Перевірка дати на відповіднісь типу str і значення формату date/month/year

    def __check_date(self, date):
        if type(date) == str:
            result = date.strip().split("/")
            if len(result) == 3 and self.__check_int(result[0]) and self.__check_int(result[1]) and self.__check_int(result[2]):
                if 1 <= int(result[0]) <= 31 and 1 <= int(result[1]) <= 12:
                    return True
            else:
                return False
        else:
            return False

    # Перевірка чи можна конвертувати отримане значення в число (float)
    def __check_float(self, amount):
        try:
            float(amount)
        except ValueError:
            return False
        else:
            return True

    #  Купляти товар з використанням карточки на знижку

    def buy_with_discount(self, amount):
        if self.__check_float(amount):
            self.__add_money(amount)

    # Додавання грошей на рахунок і перерахунок знижки

    def __add_money(self, amount):
        if self.__check_float(amount) and float(amount) > 0:
            self.__amount += float(amount)
            self.__discount = 1 + self.__amount // 1000

    #  Виводити інформацію про поточну величину знижки
    def print_discount(self):
        print("Поточна знижка: ", self.__discount, "%", sep="")

    # Виводити інформацію про те, на яку суму ще необхідно докупити товару, щоб величина знижки збільшилась.
    def print_required_amount_to_increase_discount(self):
        print("Щоб величина знижки збільшилась, ще необхідно докупити товару на суму:",
              1000 - self.__amount % 1000)

    def __str__(self):
        return "Discount Card: #"+str(self.__card_number)+", discount: "+str(self.__discount)+"%, date: "+str(self.__date)


card1 = DiscountCard(card_number=1, amount='400', date='11/01/2020')
card2 = DiscountCard(card_number=2, amount=800)
print(card1, card2, sep="\n")
print("="*51)

card1.buy_with_discount("1200")
card1.print_discount()
print(card1)
card1.print_required_amount_to_increase_discount()
print("="*51)

card2.buy_with_discount("4300")
card2.print_discount()
print(card2)
card2.print_required_amount_to_increase_discount()
print("="*51)