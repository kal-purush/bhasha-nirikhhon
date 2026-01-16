def read_file(filename):
    """Returns the contents of a file as a string"""

    return open(filename, "r").read()

def get_unique_chars(text):
    """Returns the unique characters of a string as an ordered array"""

    char_set = {c for c in text}              # selects unique chars
    char_list = sorted([c for c in char_set]) # puts them into an ordered list
    return char_list

def get_binary_length(number):
    """Returns the length of the number represented in binary"""

    return len(bin(number)[2:])

def get_binary_representation(number, length):
    """Returns the number represented in binary with padding zeroes"""

    binary = bin(number)[2:]
    padding = '0' * (length - len(binary)) if length - len(binary) > 0 else ''
    return padding + binary

def compress(string, table_max_size):
    """Compresses a string using Lempel Ziv and returns the compressed string"""

    unique_chars = get_unique_chars(string) 
    table = []
    address_map = {}
    payload = []
    last_char = string[len(string)-1]

    for c in unique_chars:
        table.append(c)
        address_map[c] = len(table)

    i = 0
    j = 0
    while i != len(string) - 1:
        address = 0
        j = i + 1

        while True:
            part = string[i:j]

            if part in address_map:
                address = address_map[part]
                j = j + 1
            
            if not (part in address_map) or j == len(string):
                table.append(part)
                table_index = len(table)
                address_map[part] = table_index
                binary_length = get_binary_length(table_index)
                binary_representation = get_binary_representation(address, binary_length)
                payload.append(binary_representation)
                i = j - 1
                break

    return "".join(unique_chars) + last_char + "".join(payload)

def decompress(string):
    """Decompresses a string using Lempel Ziv and returns the decompressed string"""

    table = []
    unique_chars = {}
    last_char = None
    payload_start = None
    result = []
    
    for i, c in enumerate(string):
        if c in unique_chars:
            last_char = c
            payload_start = i + 1
            break
        else:
            table.append(c)
            table_index = len(table)
            unique_chars[c] = table_index

    i = payload_start
    while i < len(string):
        table_index = len(table) + 1
        binary_length = get_binary_length(table_index)
        part = string[i:i+binary_length]
        address = int(part, 2)
        if i != payload_start:
            table[len(table)-1] = table[len(table)-1] + table[address-1][0]
        current_char = table[address-1]
        table.append(current_char)
        result.append(current_char)
        i += binary_length
    result.append(last_char)
    return "".join(result)

string = read_file("test_file2.txt")
#print("string:      ", string)
compressed = compress(string)
#print("compressed:  ", compressed)
decompressed = decompress(compressed)
#print("decompressed:",decompressed)
print("check:       ", string == decompressed)