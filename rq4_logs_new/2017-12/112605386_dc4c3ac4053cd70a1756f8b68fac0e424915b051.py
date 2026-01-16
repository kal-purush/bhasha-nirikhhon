import socket
import json
import threading
import ruamel.yaml as yaml

'''with open('config_yaml.yml', 'r') as open_file:
    config_yaml_encoded = open_file.read()
    condig_decoded = yaml.load(config_yaml_encoded)'''

# connection constants
my_name = "Shahar"
# host_ip_address = '10.35.77.231'
host_ip_address = '127.0.0.1'
server_input_port = 3030
server_output_port = 3031

# commands name

# code words
OK = 'OK'
OK_GOODBYE = 'ok_goodbye'
HELP = 'help'
HELP_syntax = HELP

# ERROR = 'ERROR'
# NKNOWN_COMMAND = 'Unknown command'
GOODBYE = 'goodbye'
GOODBYE_syntax = GOODBYE
SET_syntax= 'set key value'
SET = 'set'
GET_syntax ='get key'
GET = 'get'
PREFIX_syntax = 'prefix prefix_string'
PREFIX = 'prefix'

# fixed commands dictionary
commands_list = [SET_syntax, GET_syntax, PREFIX_syntax, GOODBYE_syntax, HELP_syntax]

INVALID_SYNTAX = "Invalid syntax"
ILLEGAL_INPUT = 0
LEGAL_INPUT = 1
ERROR = 'Error'
ALIVE = 'alive'
DEAD = 'dead'


class Client(object):
    def __init__(self, output_socket, input_socket, name):
        self.name = name
        self.output_socket = output_socket
        self.input_socket = input_socket
        self.input_waiting_status = False
        self.data = None
        self.connection_status=ALIVE

    def input_socket_handler(self):
        while self.connection_status == ALIVE:
            server_response = self.input_socket.recv(4096)
            if server_response == GOODBYE:
                self.input_socket.close()
                self.connection_status = DEAD
                self.communication_round(OK_GOODBYE)
            elif server_response == OK_GOODBYE:
                self.input_socket.close()
                self.connection_status = DEAD
            elif self.input_waiting_status:
                self.data = server_response
                # print type(server_response)
                # print server_response
                self.input_waiting_status = False

    def help_print(self):
        print "Available commands syntax: "
        for i in range(len(commands_list)):
            print "#{}: {}".format(i, commands_list[i])

    def command_parser(self, user_command):
        command_parsed = user_command.split()
        '''if command_parsed[0] == HELP:
            self.print_help()
        elif command_parsed[0] == SET:
            if len(command_parsed) != 3:
                print INVALID_SYNTAX
                return (ILLEGAL_INPUT, None)
        elif command_parsed[0] == '''
        return command_parsed

    def set_func(self, user_command):
        self.output_socket.sendall(user_command)

    def get_func(self, user_command):
        # sending command to server
        self.output_socket.sendall(user_command)
        self.input_waiting_status = True
        # waiting to server to replay
        while self.input_waiting_status:
            pass
        server_response = self.data
        if server_response.startswith(ERROR):
            print "Key not found."
        else:
            print "The value retrieved is: ", server_response

    def prefix(self, user_command):
        # sending command to server
        self.output_socket.sendall(user_command)
        self.input_waiting_status = True
        # waiting to server to replay
        while self.input_waiting_status:
            pass
        server_response = self.data
        server_response_json_enc = server_response
        # print type(server_response)
        # print server_response
        server_response_json_dec = json.loads(server_response_json_enc)
        if not server_response_json_dec:
            print "No key found with asserted prefix"
        else:
            print "The key:value pairs which match asserted prefix are: "
            for key, value in server_response_json_dec.items():
                print "{} : {}".format(key, value)

    def ok_goodbye(self, user_command):
        self.output_socket.sendall(user_command)

    def communication_round(self, user_command):
        parsed_command = self.command_parser(user_command)
        if (parsed_command[0] == GET) and (len(parsed_command) == 2):
            self.get_func(user_command)
            return
        elif (parsed_command[0] == SET) and (len(parsed_command) == 3):
            self.set_func(user_command)
        elif (parsed_command[0] == HELP) and (len(parsed_command) == 1):
            self.help_print()
        elif (parsed_command[0] == PREFIX) and (len(parsed_command) == 2):
            self.prefix(user_command)
        elif parsed_command[0] == OK_GOODBYE:
            self.ok_goodbye(user_command)
            self.output_socket.close()
        else:
            print INVALID_SYNTAX

    def close_connection(self):
        # sending command to server
        self.output_socket.sendall(GOODBYE)

    def handle_ui(self):
        while self.connection_status == ALIVE:
            user_command = raw_input('Enter your command, enter ' + HELP + ' for available commands list\n')
            if not self.connection_status == ALIVE:
                print "Server closed connection."
                break
            if user_command == GOODBYE:
                self.close_connection()
                while self.connection_status == ALIVE:
                    pass
                self.output_socket.close()
                print "Connection closed."
                break
            else:
                self.communication_round(user_command)


def main():

    # operating the client
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_input_address = (host_ip_address, server_input_port)
    my_socket.connect(server_input_address)
    print "Connecting to host, output: {} at port: {}".format(host_ip_address, server_input_port)

    input_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_output_address = (host_ip_address, server_output_port)
    input_socket.connect(server_output_address)
    print "Connecting to host, input: {} at port: {}".format(host_ip_address, server_output_port)

    print "Connected to server :)"

    # Protocol first stage: sending the server my name
    print "sending user name, {}, to server...".format(my_name)
    my_socket.sendall(my_name)
    # Waiting for server to respond
    
    server_response = input_socket.recv(4096)
    if server_response != OK:
        print "Name unconfirmed, server returned error: {}".format(server_response)
        return
    print "Server confirmed user name."

    client_manager = Client(my_socket, input_socket, my_name)

    handle_ui_thread = threading.Thread(target=client_manager.handle_ui,
                                        args=(),
                                        )
    handle_ui_thread.start()
    # print "started UI thread"
    handle_input_thread = threading.Thread(target=client_manager.input_socket_handler,
                                           args=(),
                                           )
    handle_input_thread.start()
    # print "started listening thread"

if __name__ == '__main__':
    main()






