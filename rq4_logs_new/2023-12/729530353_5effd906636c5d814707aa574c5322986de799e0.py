from pyredis.types import Array, BulkString, Error, Integer, SimpleString


MSG_SEPARATOR = b"\r\n"

def extract_frame_from_buffer(buffer): 

    separator = buffer.find(MSG_SEPARATOR)

    if separator == -1:
        return None, 0
    
    else: 
        payload = buffer[1:separator].decode()


    message_type = chr(buffer[0])

    if message_type == '+': # String

        return SimpleString(payload), separator + 2
    
    elif message_type == '-': # Error

        return Error(payload), separator + 2

    elif message_type == '+': # Integer
        return Integer(int(payload)), separator + 2

    return None, 0