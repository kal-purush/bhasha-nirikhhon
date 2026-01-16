
import socket
from scratchclient import ScratchSession
import threading
import time
import random
import json
import struct
import inspect

# Where is your password to connect at scratch

password_path="C:\\Users\\elois\\Desktop\\temp\\password.txt"
#What is the name of your scratch account
name ="EloiStree"

# What is the computer you want to redirect value too
host='127.0.0.1'
# What is the port (the app) that need to received those information
port=12344
# What is the computer you want to redirect index int cmd too
host_int_cmd='127.0.0.1'
# What is the port (the app) that need to received those int cmd information
port_int_cmd=12346


# What is the project ID of where the cloud var need to be read
# TDD random https://scratch.mit.edu/projects/967799973/
# Controller with cloud var https://scratch.mit.edu/projects/960534356/
# Range of test: https://scratch.mit.edu/projects/967408633
project_id=967799973


cloud_var_label_to_int = {}
cloud_var_label_to_int["☁ int_cloud_input_with_anti_Spam"]=42
cloud_var_label_to_int["☁ CV0"] = 50000
cloud_var_label_to_int["☁ CV1"] = 50001
cloud_var_label_to_int["☁ CV2"] = 50002
cloud_var_label_to_int["☁ CV3"] = 50003
cloud_var_label_to_int["☁ CV4"] = 50004
cloud_var_label_to_int["☁ CV5"] = 50005
cloud_var_label_to_int["☁ CV6"] = 50006
cloud_var_label_to_int["☁ CV7"] = 50007
cloud_var_label_to_int["☁ CV8"] = 50008
cloud_var_label_to_int["☁ CV9"] = 50009



#Example of Get Value
value = cloud_var_label_to_int["☁ int_cloud_input_with_anti_Spam"]
print(value)

def send_udp_message(message):
    try:
        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Send the message
            s.sendto(message.encode('utf-8'), (host, port))
        print(f"UDP message sent to {host}:{port}: {message}")
    except Exception as e:
        print(f"Error sending UDP message: {e}")

def read_file(file_path):
    try:
        with open(file_path, 'r') as file:
            file_content = file.read()
            return file_content
    except FileNotFoundError:
        return f"Error: File '{file_path}' not found."
    except Exception as e:
        return f"Error: {e}"


connection=""
def reconnect():
    global connection, session
    #password_path="password.txt"


    print("Read Password.")

    #password= read_file("password.txt")
    password= read_file(password_path)

    name ="EloiStree"

    print("Start connection to scratch")
    session = ScratchSession(name, password)

    connection = session.create_cloud_connection(project_id)
    connection.on("set")

reconnect()


def do_the_stuff():
    # Replace this with the action you want to perform
    try:
        varRandom= random.randint(1, 9999999)
        print(f"Ping {varRandom}")
        connection.set_cloud_variable("ServerPing",varRandom )
    except:
        reconnect()

def action_thread():
    while True:
        do_the_stuff()
        time.sleep(1)



print("Is log test:")
print(session.get_studio(34580905).description)
#print(session.get_project(project_id).get_comments()[0].content)


def send_intCmd_value(intIndex, intValue):
    # Convert text inputs to integers
    intIndex = int(intIndex)
    intValue = int(intValue)
    
    # Create a bytes array with intIndex and intValue as integers
    #data = intIndex.to_bytes(4, byteorder='big') + intValue.to_bytes(4, byteorder='big')


    ##WARNING INTEGER are not the same in binary in python or C# Apparently
    ## THIS line convert them
    data = struct.pack('<ii', intIndex, intValue)
    
    print(f"Sent{host}:{port}: {intIndex} {intValue}")
    
    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    try:
        # Send data via UDP
        sock.sendto(data, (host_int_cmd, port_int_cmd))
        print("Data sent successfully.")
    except Exception as e:
        print("Error occurred while sending data:", e)
    finally:
        # Close the socket
        sock.close()



@connection.on("set")
def on_set(variable):
    print(f"CLOUD:{variable.name}:{variable.value}")
    send_intCmd_value(cloud_var_label_to_int[variable.name],variable.value)
    try:
        
        
        variable.name=variable.name.replace("☁ ", "")


        send_udp_message(f"scratchvar:{variable.name}:{variable.value}")
        
    except:
        
        reconnect()
        connection.on("set")
    print(f"Sent Success")

    
print("Start ping Thread")
# Create a thread
thread = threading.Thread(target=action_thread)
# Start the thread
thread.start()
thread.join()

# Add any other code here if you want to do something else concurrently

# Wait for the thread to finish (you can also use thread.join() to wait for it)


#print("T2")
#connection.set_cloud_variable("Test", 42)
#print(connection.get_cloud_variable("Test"))

#print("End")