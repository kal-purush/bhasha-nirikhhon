from model.Vaccine import Vaccine
from model.Caregiver import Caregiver
from model.Patient import Patient
from util.Util import Util
from db.ConnectionManager import ConnectionManager
import pymssql
import datetime


'''
objects to keep track of the currently logged-in user
Note: it is always true that at most one of currentCaregiver and currentPatient is not null
        since only one user can be logged-in at a time
'''
current_patient = None

current_caregiver = None


def create_patient(tokens):
    
    # create the patient username and password
    # 1) check to see if length of token is exactly 3
    if len(tokens) != 3:
        print("Failed to create user.")
        return
    
    #username and password taken the index positions of 1 and 2 respectively
    username = tokens[1]       
    password = tokens[2]

    # 2) check to see if the username is in use
    if username_exists_patient(username):
        print("Username taken, try again!")
        return

    salt = Util.generate_salt()
    hash = Util.generate_hash(password, salt)
    
    # initialize patient variable
    patient = Patient(username, salt=salt, hash=hash)

    # store patient information in database
    try:
        patient.save_to_db()
    except pymssql.Error as e:
        print("Failed to create user.")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Failed to create user.")
        print(e)
        return
    print("Created user ", username)  

def username_exists_patient(username):
    cm = ConnectionManager()
    conn = cm.create_connection()

    select_username = "SELECT * FROM Patient WHERE Username = %s"
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(select_username, username)
        #  returns false if the cursor is not before the first record or if there are no rows in the ResultSet.
        for row in cursor:
            return row['Username'] is not None
    except pymssql.Error as e:
        print("Error occurred when checking username")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Error occurred when checking username")
        print("Error:", e)
    finally:
        cm.close_connection()
    return False

def create_caregiver(tokens):
    # create_caregiver <username> <password>
    # check 1: the length for tokens need to be exactly 3 to include all information (with the operation name)
    if len(tokens) != 3:
        print("Failed to create user.")
        return

    username = tokens[1]
    password = tokens[2]
    # check 2: check if the username has been taken already
    if username_exists_caregiver(username):
        print("Username taken, try again!")
        return

    salt = Util.generate_salt()
    hash = Util.generate_hash(password, salt)

    # create the caregiver
    caregiver = Caregiver(username, salt=salt, hash=hash)

    # save to caregiver information to our database
    try:
        caregiver.save_to_db()
    except pymssql.Error as e:
        print("Failed to create user.")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Failed to create user.")
        print(e)
        return
    print("Created user ", username)


def username_exists_caregiver(username):
    cm = ConnectionManager()
    conn = cm.create_connection()

    select_username = "SELECT * FROM Caregivers WHERE Username = %s"
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(select_username, username)
        #  returns false if the cursor is not before the first record or if there are no rows in the ResultSet.
        for row in cursor:
            return row['Username'] is not None
    except pymssql.Error as e:
        print("Error occurred when checking username")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Error occurred when checking username")
        print("Error:", e)
    finally:
        cm.close_connection()
    return False


def login_patient(tokens):
    # login_patient <username> <password>
    # check 1: if someone's already logged-in, they need to log out first
    global current_patient
    if current_patient is not None or current_caregiver is not None:
        print("User already logged in.")
        return

    # check 2: the length for tokens need to be exactly 3 to include all information (with the operation name)
    if len(tokens) != 3:
        print("Login failed.")
        return

    username = tokens[1]
    password = tokens[2]

    patient = None
    try:
        patient = Patient(username, password=password).get()
    except pymssql.Error as e:
        print("Login failed.")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Login failed.")
        print("Error:", e)
        return

    # check if the login was successful
    if patient is None:
        print("Login failed.")
    else:
        print("Logged in as: " + username)
        current_patient = patient



def login_caregiver(tokens):
    # login_caregiver <username> <password>
    # check 1: if someone's already logged-in, they need to log out first
    global current_caregiver
    if current_caregiver is not None or current_patient is not None:
        print("User already logged in.")
        return

    # check 2: the length for tokens need to be exactly 3 to include all information (with the operation name)
    if len(tokens) != 3:
        print("Login failed.")
        return

    username = tokens[1]
    password = tokens[2]

    caregiver = None
    try:
        caregiver = Caregiver(username, password=password).get()
    except pymssql.Error as e:
        print("Login failed.")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Login failed.")
        print("Error:", e)
        return

    # check if the login was successful
    if caregiver is None:
        print("Login failed.")
    else:
        print("Logged in as: " + username)
        current_caregiver = caregiver


def search_caregiver_schedule(tokens):
    global current_patient
    
    global current_caregiver
    
    #  check 1: check to see if caregiver is logged in
    if current_caregiver is None and current_patient is None:
        print("Please login first!")
        return

     # check 2: to see if the length of tokens is equals to 2, if it isnt then print error message
    if len(tokens) != 2:
        print("Please try again!")
        return

    cm = ConnectionManager()
    conn = cm.create_connection()
    cursor = conn.cursor(as_dict=True)


    #pull the schedule to see the availability of the caregivers
    available_caregivers = "SELECT Username FROM Availabilities WHERE Time = %s ORDER BY Username"
    
    date = tokens[1]
    date_tokens = date.split("-")
    month = int(date_tokens[0])
    day = int(date_tokens[1])
    year = int(date_tokens[2])

    #selecting vaccines
    find_vaccine = "SELECT Name, Doses FROM Vaccines"

    try:  
        d = datetime.datetime(year, month, day)
        cursor.execute(available_caregivers, d)
        print("These are the available caregivers: " )
        for row in cursor:
            print(row['Username'])
        
        print()

        cursor.execute(find_vaccine)
        print("These are the vax's avilable: ")
        for row in cursor:
            print(row['Name'], " ", row['Doses'])
        
        cm.close_connection()
    except pymssql.Error as e:
        print("Please try again!", e)
    except Exception as e:
        print("Please try again!", e)
    except ValueError:
        print("Please enter a valid date!")
        return
    finally:
        cm.close_connection()


def reserve(tokens):
    global current_patient
    
    global current_caregiver
    # reserve appoint for patient based on caregiver and vaccine doses
    # check 1; check to see if no user is logged in. if they arent then print message
    if current_caregiver is None and current_patient is None:
        print("Please login first!")
        return
    elif current_caregiver is not None and current_patient is None:
        print("Please login as a patient!")
        return
    
    #  check 2: this will check if the length of tokens is exactly 3 
    if len(tokens) != 3:
        print("Please try again!")
        return
    
    cm = ConnectionManager()
    conn = cm.create_connection()
    cursor = conn.cursor(as_dict=True)
    patient_name = current_patient.get_username()

    # selecting top user as caregivers can only see one per day
    available_caregivers = "SELECT TOP 1 Username FROM Availabilities WHERE Time = %s ORDER BY Username"
    
    date = tokens[1]
    date_tokens = date.split("-")
    month = int(date_tokens[0])
    day = int(date_tokens[1])
    year = int(date_tokens[2])

    
    #selects doses
    vaccine = tokens[2]
    vaccine_name = "SELECT Doses FROM Vaccines WHERE Name = %s"

    #creates appointment in system
    appointment = "INSERT INTO Appointment VALUES (%s, %s, %s, %s)"
    patient_name = current_patient.get_username()
    appointment_id = "SELECT Appointment_id FROM Appointment WHERE P_Username = %s AND Time = %s"

    try:
        cursor.execute(vaccine_name, vaccine)
        for row in cursor:
            num_doses = row['Doses']
            if num_doses <= 0:
                print("Not enough available doses!")
                return None

        d = datetime.datetime(year, month, day)
        cursor.execute(available_caregivers, d)
        row = cursor.fetchone()
        c_name = row["Username"]
        if row == None:
            print("No Caregiver is avaible!")
            cm.close_connection()    
            return 
        else:            
            # this will store the  appointment info in the  appointment table
            cursor.execute(appointment, (vaccine, d,  c_name, patient_name))
            conn.commit()
            #this will print the appointment id
            cursor.execute(appointment_id, (patient_name, d))
            id_row = cursor.fetchone()
            id = id_row['Appointment_id']
            print("Appointment ID: " , id , ", " , "Caregiver username: " , c_name) 
    
            cm.close_connection()    
    #used to handle errors    
    except pymssql.Error as e:
        print("Please try again!", e)
    except Exception as e:
        print("Please try again!", e)


def upload_availability(tokens):
    #  upload_availability <date>
    #  check 1: check if the current logged-in user is a caregiver
    global current_caregiver
    if current_caregiver is None:
        print("Please login as a caregiver first!")
        return

    # check 2: the length for tokens need to be exactly 2 to include all information (with the operation name)
    if len(tokens) != 2:
        print("Please try again!")
        return

    date = tokens[1]
    # assume input is hyphenated in the format mm-dd-yyyy
    date_tokens = date.split("-")
    month = int(date_tokens[0])
    day = int(date_tokens[1])
    year = int(date_tokens[2])
    try:
        d = datetime.datetime(year, month, day)
        current_caregiver.upload_availability(d)
    except pymssql.Error as e:
        print("Upload Availability Failed")
        print("Db-Error:", e)
        quit()
    except ValueError:
        print("Please enter a valid date!")
        return
    except Exception as e:
        print("Error occurred when uploading availability")
        print("Error:", e)
        return
    print("Availability uploaded!")


def cancel(tokens):
    global current_patient
    
    global current_caregiver
    
    # allowing the cancellation option for both caregiver and patient
    # check to see if they are logged in
    if current_caregiver is None and current_patient is None:
        print("Please login first!")
        return

    cm = ConnectionManager()
    conn = cm.create_connection()
    cursor = conn.cursor()

    #this will be the query that will allow both parties to cancel
    app_id = tokens[1]
    cancel_appointment = "DELETE FROM Appointment WHERE Appointment_id = %d"

    #confirmation message of cancellation
    try:
        cursor.execute(cancel_appointment, app_id)
        conn.commit()
        print("Appointment cancelled!" + " "+  "Appointment ID: " + app_id)
    except pymssql.Error:
        raise
    finally:
        cm.close_connection()


def add_doses(tokens):
    #  add_doses <vaccine> <number>
    #  check 1: check if the current logged-in user is a caregiver
    global current_caregiver
    if current_caregiver is None:
        print("Please login as a caregiver first!")
        return

    #  check 2: the length for tokens need to be exactly 3 to include all information (with the operation name)
    if len(tokens) != 3:
        print("Please try again!")
        return

    vaccine_name = tokens[1]
    doses = int(tokens[2])
    vaccine = None
    try:
        vaccine = Vaccine(vaccine_name, doses).get()
    except pymssql.Error as e:
        print("Error occurred when adding doses")
        print("Db-Error:", e)
        quit()
    except Exception as e:
        print("Error occurred when adding doses")
        print("Error:", e)
        return

    # if the vaccine is not found in the database, add a new (vaccine, doses) entry.
    # else, update the existing entry by adding the new doses
    if vaccine is None:
        vaccine = Vaccine(vaccine_name, doses)
        try:
            vaccine.save_to_db()
        except pymssql.Error as e:
            print("Error occurred when adding doses")
            print("Db-Error:", e)
            quit()
        except Exception as e:
            print("Error occurred when adding doses")
            print("Error:", e)
            return
    else:
        # if the vaccine is not null, meaning that the vaccine already exists in our table
        try:
            vaccine.increase_available_doses(doses)
        except pymssql.Error as e:
            print("Error occurred when adding doses")
            print("Db-Error:", e)
            quit()
        except Exception as e:
            print("Error occurred when adding doses")
            print("Error:", e)
            return
    print("Doses updated!")


def show_appointments(tokens):
    global current_patient
   
    global current_caregiver

    #check 1: check for login info , if nothing then display error messages
    if current_caregiver is None and current_patient is None:
        print("Please login first!")
        return
    
    cm = ConnectionManager()
    conn = cm.create_connection()
    cursor = conn.cursor(as_dict=True)

    #retrieving info from caregiver and patient information in order to display appointments 
    caregiver_info = "SELECT Appointment_id, Name, Time,P_Username FROM Appointment WHERE C_Username = %s ORDER BY Appointment_id"
    patient_info = "SELECT Appointment_id, Name, Time,C_Username FROM Appointment WHERE P_Username = %s ORDER BY Appointment_id"

    
    #check 2: in the instance that there are appointments, this will output that
    check_2 = "SELECT * FROM Appointment"
    #overall used to handle continours errors
    try:
        cursor.execute(check_2)
        row = cursor.fetchone()
        #checks to see if you have an appointment, if you dont then it wont run anything
        if row == None:
            print("You don't have an appointment")
            cm.close_connection()    
            return 
        else:     
            if current_caregiver is not None and current_patient is None:
                cg_name = current_caregiver.get_username()
                cursor.execute(caregiver_info, cg_name)
                print('Appointment_id:', " ", 'Vaccine:', " ", 'Time:', " ", 'Patient Name:' )
                print()
                
                for row in cursor:
                    c_appoint_id = row['Appointment_id']
                    c_Name = row['Name']
                    c_Time = row['Time']
                    c_P_Username = row['P_Username']
                    print(c_appoint_id, " ", c_Name, " ", c_Time, " ", c_P_Username)
                cm.close_connection()   
            
            elif current_caregiver is None and current_patient is not None:
                patient_name = current_patient.get_username()
                cursor.execute(patient_info, patient_name)
                print('Appointment_id:', " ", 'Vaccine:', " ", 'Time:', " ", 'Caregivers Name:' )
                print()
                
                for row in cursor:
                        c_appoint_id = row['Appointment_id']
                        c_Name = row['Name']
                        c_Time = row['Time']
                        c_C_Username = row['C_Username']
                        print(c_appoint_id, " ", c_Name, " ", c_Time, " ", c_C_Username )
                cm.close_connection()   
    
    #to handle all other errors, this will print the desired error message
    except pymssql.Error as e:
        print("Please try again!", e)
    except Exception as e:
        print("Please try again!", e)
    finally:
        cm.close_connection()      


def logout(tokens):
    global current_patient
    
    global current_caregiver
    #check 1: if not logged in, print error message of condition is satisfied
    
    if current_caregiver is None and current_patient is None:
        print("Please login first.")
    else:
        #able to logout, print confirmation message. else alert to try again
        try:
            print("Successfully logged out!")
            current_caregiver = None
            current_patient = None
            return
        except pymssql.Error as e:
            print("Please try again!", e)
        except Exception as e:
            print("Please try again!", e)  


def start():
    stop = False
    print()
    print(" *** Please enter one of the following commands *** ")
    print("> create_patient <username> <password>")  # //TODO: implement create_patient (Part 1)
    print("> create_caregiver <username> <password>")
    print("> login_patient <username> <password>")  # // TODO: implement login_patient (Part 1)
    print("> login_caregiver <username> <password>")
    print("> search_caregiver_schedule <date>")  # // TODO: implement search_caregiver_schedule (Part 2)
    print("> reserve <date> <vaccine>")  # // TODO: implement reserve (Part 2)
    print("> upload_availability <date>")
    print("> cancel <appointment_id>")  # // TODO: implement cancel (extra credit)
    print("> add_doses <vaccine> <number>")
    print("> show_appointments")  # // TODO: implement show_appointments (Part 2)
    print("> logout")  # // TODO: implement logout (Part 2)
    print("> Quit")
    print()
    while not stop:
        response = ""
        print("> ", end='')

        try:
            response = str(input())
        except ValueError:
            print("Please try again!")
            break

        response = response.lower()
        tokens = response.split(" ")
        if len(tokens) == 0:
            ValueError("Please try again!")
            continue
        operation = tokens[0]
        if operation == "create_patient":
            create_patient(tokens)
        elif operation == "create_caregiver":
            create_caregiver(tokens)
        elif operation == "login_patient":
            login_patient(tokens)
        elif operation == "login_caregiver":
            login_caregiver(tokens)
        elif operation == "search_caregiver_schedule":
            search_caregiver_schedule(tokens)
        elif operation == "reserve":
            reserve(tokens)
        elif operation == "upload_availability":
            upload_availability(tokens)
        elif operation == cancel:
            cancel(tokens)
        elif operation == "add_doses":
            add_doses(tokens)
        elif operation == "show_appointments":
            show_appointments(tokens)
        elif operation == "logout":
            logout(tokens)
        elif operation == "quit":
            print("Bye!")
            stop = True
        else:
            print("Invalid operation name!")


if __name__ == "__main__":
    '''
    // pre-define the three types of authorized vaccines
    // note: it's a poor practice to hard-code these values, but we will do this ]
    // for the simplicity of this assignment
    // and then construct a map of vaccineName -> vaccineObject
    '''

    # start command line
    print()
    print("Welcome to the COVID-19 Vaccine Reservation Scheduling Application!")

    start()