# Concrete classes for payment methods:

from interfaces.payment_processing_interface import PaymentProcessingInterface


class BankTransferPayment(PaymentProcessingInterface):

    def process_payment(self, amount: float):
        print(f"Processing bank transfer for amount: {amount}")


class PayPalPayment(PaymentProcessingInterface):

    def process_payment(self, amount: float):
        print(f"Processing PayPal payment for amount: {amount}")


class VisaCardPayment(PaymentProcessingInterface):

    def process_payment(self, amount: float):
        print(f"Processing Visa card payment for amount: {amount}")


class InstallmentPayment(PaymentProcessingInterface):

    def process_payment(self, amount: float):
        print(f"Processing installment payment for amount: {amount} in 4 parts")