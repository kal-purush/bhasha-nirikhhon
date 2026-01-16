import traceback
from datetime import datetime
from datetime import date
import psycopg2
import creds
from psycopg2 import Error
from psycopg2 import sql
# Connect to an existing database
connection = psycopg2.connect(user=creds.user,
                                password=creds.password,
                                host="web0.eecs.uottawa.ca",
                                port=creds.port,
                                database="group_b02_g20")

print("connected")

# Create a cursor to perform database operations
cursor = connection.cursor()
# Print PostgreSQL details

# Executing a SQL query
def adminView():

    while True:
        request = int(input("welcome to the admin menu, select one of the following options: \n1.Custom SQL Query \n"))
        if request==1:
            query = str(input("Please enter your custom query"))
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                print("SIN NUMBER =", row[0])

        else:
            print("")
            print("Please insert one of the integer choices above only\n")
            continue




def guestView():
    sin_number = None
    while True:
        access= int(input("Welcome to the guest menu, select one of the following options: \n1.Signup \n2.I already have an existing customer account \n3.Exit Menu\n"))
        if(access==1):
            print("Please enter the following information:")
            sin_number = int(input("what is your sin number?"))
            first_name = str(input("what is your first name"))
            middle_name = str(input("what is your middle name?"))
            last_name = str(input("what is your last name?"))
            street_numbers = int(input("what is your street number?"))
            street= str(input("what is your street"))
            city = str(input("what is your city?"))
            province = str(input("what is your province?"))
            country = str(input("what is your country?"))
            registration_date= date.today()
            registration_date = registration_date.strftime("%Y-%m-%d")
            cursor.execute(sql.SQL("INSERT INTO customer VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(
                sql.Identifier("group_b02_g20)")),
                           [sin_number,first_name,middle_name,last_name,street_numbers,street,city,province,country,registration_date])
            connection.commit()
        elif(access == 2):
            sin_number = int(input("what is your sin number?"))

            while True:
                request = int(input("Select one of the following options: \n1.Search for available rooms \n2.Book a room \n"))
                if(request==1):
                    try:
                        hotel_id= int(input("enter the hotel id for the hotel you would like to book"))
                        start_date = str(input("what is the preffered start date for the rooms you would like to see in the format(YYYY,MM,DD)"))


                        end_date = str(input("what is the preffered end date for the rooms you would like to see in the format(YYYY,MM,DD)"))


                        cursor.execute(sql.SQL("SELECT room.room_id, room.room_capacity, hotel_chain.hotel_id, booking_info.end_date from booking_info join room ON booking_info.room_id!=room.room_id join hotel_chain ON hotel_chain.hotel_id=%s AND(%s < booking_info.end_date OR %s<booking_info.start_date OR booking_info.room_id!=room.room_id)").format(
                            sql.Identifier("group_b02_g20)")),
                            [hotel_id,start_date,end_date])

                        rows = cursor.fetchall()
                        for row in rows:
                            print("room_id =", row[0],"capacity = ", row[1],)
                    except TypeError:
                        print("Error entering your information")


                elif(request==2):
                    try:

                        booking_number = generateBookingNumber()
                        print(booking_number)
                        # sin_number = int(input("what is your sin number?"))
                        room_id = int(input("what is the room id?"))
                        hotel_id = int(input("what is the hotel id?"))
                        start_date = str(input("what is the start date for this booking in the format(YYYY,MM,DD)"))
                        start_date= datetime.strptime(start_date,"%Y-%m-%d")
                        end_date = str(input("what is the end date for this booking in the format(YYYY,MM,DD)"))
                        end_date = datetime.fromisoformat(end_date)
                        room_type = fetchRoomType(room_id)
                        room_occupants = fetchRoomOccupants(room_id)
                        cursor.execute(sql.SQL("INSERT INTO booking_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s)").format(sql.Identifier("group_b02_g20)")),[booking_number,sin_number,room_id,hotel_id,start_date,end_date,room_type,room_occupants])
                        connection.commit()
                        print("You room has been booked")
                        break
                        # cursor.execute(f'INSERT INTO booking_info(booking_number,sin_number,room_id,hotel_id,start_date,end_date,room_occupants) VALUES ({booking_number},{sin_number},{room_id},{hotel_id},{start_date},{end_date},{room_occupants})')
                    except:
                        print("error")




                else:
                    print("")
                    print("Please insert one of the integer choices above only\n")
                    continue
        break

def generateBookingNumber():
    cursor.execute("SELECT MAX(booking_info.booking_number) from booking_info")
    value=(cursor.fetchone())
    if value[0] == None:
        bookingNumber = 1000
    else:

        max = value[0]
        bookingNumber = max+1
    return bookingNumber
    # bookingNumber = max[0] = max[0] +1
    # print(bookingNumber)

def fetchRoomType(room_id):
    cursor.execute(f'SELECT room.view_type from room WHERE room.room_id={room_id}')
    value = (cursor.fetchone())
    room_type = str(value[0])
    return room_type

def fetchRoomOccupants(room_id):
    cursor.execute(f'SELECT room.room_capacity from room WHERE room.room_id={room_id}')
    value = (cursor.fetchone())
    room_capacity = str(value[0])
    return room_capacity

def employeeView():
    while True:
        sin_number = int(input("Welcome to the Employee, please enter your sin number to sign in\n"))
        cursor.execute(sql.SQL(
            "SELECT employee.sin_number from employee WHERE employee.sin_number=%s").format(
            sql.Identifier("group_b02_g20)")),
            [sin_number])
        value = cursor.fetchone()
        if value==None:
            print("Your sin number is invalid/not registered please enter it again or sign in as an admin to register your employee account\n")
        else:

            while True:
                request = int(input("Select one of the following options: \n1.Search for availaible rooms \n2.Search for unavailable rooms \n3.Transform a booking into a rental agreement \n4.Exit Menu"))
                if(request==1):
                    hotel_id = int(input("enter the hotel id for the hotel you would like to book"))
                    start_date = str(
                        input("what is the preffered start date for the rooms you would like to see in the format(YYYY,MM,DD)"))

                    end_date = str(
                        input("what is the preffered end date for the rooms you would like to see in the format(YYYY,MM,DD)"))

                    cursor.execute(sql.SQL(
                        "SELECT room.room_id, room.room_capacity, hotel_chain.hotel_id, booking_info.end_date from booking_info join room ON booking_info.room_id!=room.room_id join hotel_chain ON hotel_chain.hotel_id=%s AND(%s > booking_info.end_date AND %s<booking_info.start_date OR booking_info.room_id!=room.room_id)").format(
                        sql.Identifier("group_b02_g20)")),
                        [hotel_id, start_date, end_date])

                    rows = cursor.fetchall()
                    for row in rows:
                        print("room_id =", row[0], "capacity = ", row[1], )

                elif(request==2):
                    hotel_id = int(input("enter the hotel id for the hotel you would like to book"))
                    start_date = str(
                        input("what is the preffered start date for the rooms you would like to see in the format(YYYY,MM,DD)"))

                    end_date = str(
                        input("what is the preffered end date for the rooms you would like to see in the format(YYYY,MM,DD)"))

                    cursor.execute(sql.SQL(
                        "SELECT room.room_id, room.room_capacity, hotel_chain.hotel_id, booking_info.end_date from booking_info join room ON booking_info.room_id!=room.room_id join hotel_chain ON hotel_chain.hotel_id=%s AND(%s <booking_info.end_date OR %s>booking_info.start_date AND booking_info.room_id=room.room_id)").format(
                        sql.Identifier("group_b02_g20)")),
                        [hotel_id, start_date, end_date])

                    rows = cursor.fetchall()
                    for row in rows:
                        print("room_id =", row[0], "capacity = ", row[1], )
                elif(request==3):
                    booking_number = int(input("What is the booking number?"))
                    signed_date = date.today()
                    signed_date = signed_date.strftime("%Y-%m-%d")

                    cursor.execute(sql.SQL("INSERT INTO rental_agreement VALUES(%s,%s)").format(
                        sql.Identifier("group_b02_g20)")),
                        [booking_number, signed_date])
                    connectiom.commit()
                elif(request==4):
                    break
            break



def main():
    while True:
        access = int(input("Please specify your what kind of user you are:\n 1.Admin\n 2.Guest\n 3.Employee\n 4.Exit Menu\n"))
        if(access==1):
            adminView()
        elif(access==2):
            guestView()
        elif(access==3):
            employeeView()
        elif(access==4):
            break
        else:
            print("")
            print("Please insert one of the 3 integer choices above only\n")
            continue
if __name__ == '__main__':
    main()



