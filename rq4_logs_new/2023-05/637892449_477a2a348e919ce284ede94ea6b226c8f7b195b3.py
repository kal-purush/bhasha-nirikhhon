class ParkingGarage:
    def __init__(self):
        self.tickets = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.parkingSpaces = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.currentTicket = {}

    def takeTicket(self):
        if self.tickets and self.parkingSpaces:
            ticket_number = self.tickets.pop(0)
            parking_space = self.parkingSpaces.pop(0) # Takes the first index from the list
            self.currentTicket[ticket_number] = {'paid': False, 'parking_space': parking_space} # adds a dictionary to the ticket number key with 2 keys "paid" and "parking space"
            print(f"Ticket {ticket_number} taken. Please proceed to parking space {parking_space}.")
        else:
            print("Sorry, no tickets or parking spaces available.")

    def payForParking(self): 
        ticket_number = int(input("Enter your ticket number: "))
        payment = input("Please pay for your ticket here: ")
        if payment == 'payment':
            print("Thank you, your payment has been received.")
        else:
            print("Invalid payment. Please try again.")
        if ticket_number in self.currentTicket and not self.currentTicket[ticket_number]['paid']: # checks to see if ticket number value is in dictionary and checks to see if the ticket is paid for
            self.currentTicket[ticket_number]['paid'] = True
            print("Payment successful. You have 15 minutes to leave.")
            return ticket_number
        else:
            print("Invalid ticket number or ticket already paid.")

