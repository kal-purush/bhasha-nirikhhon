import abc
from datetime import datetime

class Car(object):
    def __init__(self):
        self.ticket = None
        #critique ==> car has ticket attribute although it is not necessary that parking lot
        # in which the car is parked will work with tickets. There can be free parking lots
        # or company paid parking lots which don't require you to have ticket

class SmallCar(Car):
    def __init__(self):
        super(SmallCar, self).__init__()

class LargeCar(Car):
    def __init__(self):
        super(LargeCar, self).__init__()

#we support credit card & cash payment
class PaymentOption(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def process_payment(self, amount):
        pass

class CashPaymentOption(PaymentOption):

    def process_payment(self, amount):
        print("cash {} accepted as payment")

class CreditCardPaymentOption(PaymentOption):

    def process_payment(self, amount):
        print("credit card billed {} as payment")

'''
                                          |-------->CreditCardPayment
    client  -----> paymentGatewayProxy ---| 
                                          |-------->CashPayment

    There are multiple ways to pay, using credit cards or cash etc.
    This complexity is hidden from client using paymentGatewayProxy
    the client only interacts with paymentGatewayProxy
'''
class PaymentGatewayProxy(CreditCardPaymentOption, CashPaymentOption):
    def __init__(self):
        self.card_payment = CreditCardPaymentOption()
        self.cash_payment = CashPaymentOption()

    def process_payment(self, payment):
        if isinstance(payment, CashPayment):
            self.cash_payment.process_payment(payment.amount)
        elif isinstance(payment, CreditCardPayment):
            self.card_payment.process_payment(payment.amount)

class CashPayment(object):
    def __init__(self, amount):
        self.amount = amount

class CreditCardPayment(object):
    def __init__(self, amount):
        self.amount = amount

"""
    How will end user or parked car pay for the parking.
    Will support below 3-4 types of scenarios:
    1. Absolutely free (we just keep track of number of cars and bill some company later)
    but no car is charged
    2. Fixed fees are paid per parking
    3. Dynamic fees based on time
    
    We will also support
        - corporate discount
        - weekend pricing
        for (3) dynamic fees
"""
class MonitizingPolicy(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def pre_checkin(self, car):
        pass

    @abc.abstractmethod
    def post_checkin(self, car):
        pass

    @abc.abstractmethod
    def pre_checkout(self, car):
        pass

    @abc.abstractmethod
    def post_checkout(self, car):
        pass


#we have parking lot and we don't change anything
# to park a car. no matter time or car size
class FreeMonitozingPolicy(MonitizingPolicy):
    def __init__(self):
        self.total_cars_parked = 0
        self.total_cars_left = 0

    def pre_checkin(self, car):
        pass

    def post_checkin(self, car):
        self.total_cars_parked += 1

    def pre_checkout(self, car):
        pass

    def post_checkout(self, car):
        self.total_cars_left += 1

# no matter the time or car size, pay a fixed $$
# as parking fees
class FixedMonitozingPolicy(MonitizingPolicy):

    def pre_checkin(self, car):
        pass

    def post_checkin(self, car):
        pass

    def pre_checkout(self, car):
        pass

    def post_checkout(self, car):
        pass

#charge is based on time
class TicketBasedMonitizingPolicy(MonitizingPolicy):
    def __init__(self, **kwargs):
        #@TODO construct the rules object here
        #we can use builder pattern here
        self.charges = [TimeBasedRate(rate=kwargs.get("rate"), hours=kwargs.get("hours")),
                        CorpDiscountRate()]

    def pre_checkin(self, car):
        ticket = Ticket()
        ticket.checkin_time = datetime.now()
        car.ticket = ticket

    def post_checkin(self, car):
        pass

    def pre_checkout(self, car):
        return self.calculate_payment()

    def post_checkout(self, car):
        car.ticket = None

    def calculate_payment(self):
        payment = 0
        for charge in self.charges:
            payment += charge.rate()
        return payment

# - incorporate corporate discount
# - special rate for special days like public holidays
# - 1st 1 Hr free then 4$ for next 2 hrs, $20 after that

class BaseRate(object):
    def __init__(self, amount):
        self.rate = amount

class TimeBasedRate(object):
    def __init__(self, rate=10, hours=4):
        self.base_rate = rate
        self.hours = hours
        self.checkin_time = None
        self.checkout_time = None

    def rate(self):
        return self.apply_rules()

    def apply_rules(self):
        if self.checkout_time - self.checkin_time < self.hours:
            return self.base_rate
        else:
            return self.base_rate + 20

class CorpDiscountRate(object):
    def __init__(self):
        self.rate = None

    def rate(self):
        return self.apply_discount()

    def apply_discount(self):
        return 10

class WeekendDiscountRate(object):
    def __init__(self):
        self.rate = None

    def rate(self):
        return self.weekend_premium()

    def weekend_premium(self):
        return 50

class BaseParkingLot(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def is_full(self):
        pass

    @abc.abstractmethod
    def is_space_available(self, car):
        pass

    @abc.abstractmethod
    def occupy_space(self, car):
        pass

    @abc.abstractmethod
    def leave_space(self, car):
        pass

    @abc.abstractmethod
    def pre_checkin(self, car):
        pass

    @abc.abstractmethod
    def post_checkin(self, car):
        pass

    @abc.abstractmethod
    def pre_checkout(self, car):
        pass

    @abc.abstractmethod
    def post_checkout(self, car):
        pass

    def checkin(self, car):
        self.pre_checkin(car)
        if self.is_space_available(car) is True:
            self.occupy_space(car)
        self.post_checkin(car)

    def checkout(self, car):
        self.pre_checkout(car)
        self.leave_space(car)
        self.post_checkout(car)
        #pay or don't pay based on monitisation policy
        #indicate that the space is empty

class Ticket(object):
    def __init__(self):
        self.checkin_time = None
        self.checkout_time = None

class ParkingLotA(BaseParkingLot):
    def __init__(self):
        super(ParkingLotA, self).__init__()
        self.payment_proxy = PaymentGatewayProxy()
        self.monit_policy = TicketBasedMonitizingPolicy(rate=5, hours=1)
        self.MAX_SMALL_CARS = 5
        self.MAX_LARGE_CARS = 5
        self.curr_small_cars = 0
        self.curr_large_cars = 0

    def pre_checkin(self, car):
        self.monit_policy.pre_checkin(car)

    def post_checkin(self, car):
        pass

    def pre_checkout(self, car):
        return self.monit_policy.pre_checkout(car)

    def post_checkout(self, car):
        self.monit_policy.post_checkout(car)

    def pay(self, paymentOption):
        self.payment_proxy.process_payment(paymentOption)

    def leave_space(self, car):
        if isinstance(car, SmallCar) is True:
            self.curr_small_cars -= 1
        elif isinstance(car, LargeCar) is True:
            self.curr_large_cars -= 1

    def occupy_space(self, car):
        if isinstance(car, SmallCar) is True:
            self.curr_small_cars += 1
        elif isinstance(car, LargeCar) is True:
            self.curr_large_cars += 1

    def is_space_available(self, car):
        if isinstance(car, SmallCar) is True:
            if self.curr_small_cars >= self.MAX_SMALL_CARS:
                return False
            else:
                return True
        elif isinstance(car, LargeCar) is True:
            if self.curr_large_cars >= self.MAX_LARGE_CARS:
                return False
            else:
                return True

class ParkingLotB(BaseParkingLot):
    def __init__(self):
        super(ParkingLotB, self).__init__()
        self.payment_proxy = PaymentGatewayProxy()
        self.monit_policy = TicketBasedMonitizingPolicy()
        self.MAX_SMALL_CARS = 4
        self.curr_small_cars = 0