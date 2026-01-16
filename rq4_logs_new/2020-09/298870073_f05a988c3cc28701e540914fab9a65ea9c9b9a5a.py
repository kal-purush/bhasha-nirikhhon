from sys import argv as arg  # for working with args of CLI
import time


def search_subnet():
    ''' The function defines the min subnet for received IP-addresses (IPv4 or IPv6).

    Tre function is called with two arguments of CLI.
    Arguments of CLI:
    sys.argv[1] is file name with IP-addresses
    sys.argv[2] is type of IP-addresses (IPv4 or IPv6)
    The received data is checked for correctness.
    If the check fails, the function exists and returns the None.
    Else, data processing is performed using auxiliary functions:
    - check_type_of_ip(type_of_ip);
    - taking_kit_of_addresses(file_name);
    - get_addr_by_octets(arr_with_addr: list, type_of_ip: str);
    - check_correct_of_addr(addr: list, type_of_ip: str);
    - search_octet_with_host(addr: list);
    - search_max_num_in_octets(addr: list, octet_with_host: int);
    - get_mask(num: int, type_of_ip: str, octet_with_host: int);
    - get_net_octet(num: list, bit_for_host: int, type_of_ip: str);
    - get_result(addr: list, net_octet: str, octet_with_host: int, mask: str, type_of_ip: str).
    The maximum number of bits for hosts from the received set is determined.
    The mask is calculated.
    Tre network  is calculated.
    Result of this function is min subnet for received addresses.
    The algorithm takes into account the type of received addresses.
    :return: a str with min subnet for received addresses.

    '''

    addresses: list[str] = []  # for IP-addresses from file

    # 1 - check sys.argv of CLI:
    if len(arg) != 3:                                  # check count of sys.argv, can to be 3
        print("The number of parameters does not correspond to the required for this function!")
        return

    file_name: str = arg[1]
    type_of_ip: str = arg[2].lower()                   # for register insensitive

    # 2 - checking type of IP-addresses
    if not check_type_of_ip(type_of_ip):
        return

    # 3 - reading file and taking a kit of addresses
    addresses = taking_kit_of_addresses(file_name)
    if not addresses:
        return

    # 4 - getting addresses by octets for next step
    addresses: list[list[str]] = get_addr_by_octets(addresses, type_of_ip)

    # 5 - checking correct of addresses from file
    if not check_correct_of_addr(addresses, type_of_ip):
        print('These addresses are incorrect!')
        return

    # 6 - searching an octet/hextet number with different meanings
    octet_with_host: int = search_octet_with_host(addresses)
    if not octet_with_host:                              # if all addresses are equal
        print("All addresses are equal. I can't find the min subnet!")
        return

    # 7 - search max number in octets/hextets with different meanings
    max_num_of_host: int = search_max_num_in_octets(addresses, octet_with_host)

    # 8 - count of bit!=0 for this max number -> mask for these addresses
    mask, bin_num, bit_for_host = get_mask(max_num_of_host, type_of_ip, octet_with_host)
    # type mask: str, bin_num: list[str], bit_for_host: int

    # 9 - searching a network part of the addresses
    net_octet: str = get_net_octet(bin_num, bit_for_host, type_of_ip)

    # 10 - result = address_network + mask
    return get_result(addresses[0], net_octet, octet_with_host, mask, type_of_ip)    # type: str


def check_type_of_ip(type_of_ip: str):                         # for step 2 - check type of IP-addresses
    if type_of_ip != 'ipv4' and type_of_ip != 'ipv6':
        print("Incorrect type of IP-addresses!")
        return False                                           # type: bool

    return True                                                # type: bool


def taking_kit_of_addresses(file_name: str):                   # for step 3
    arr: list = []
    try:
        with open(file_name, 'r') as f:
            for line in f:
                line = line.replace('\n', '')                  # for taking only address
                arr.append(line)

    except FileNotFoundError:
        print(f'No such file or directory: "{file_name}"')
        return                                                 # return None

    return arr                                                 # type: list


def get_addr_by_octets(arr_with_addr: list, type_of_ip: str):   # for step 4   type of arr_with_addr: list[str]
    addr_by_octets: list[list[str]] = []

    if type_of_ip == 'ipv4':
        for addr in arr_with_addr:
            addr_by_octets.append(addr.split('.'))       # [['192', '168', '1', '2' ], ['192', '168', '1', '3' ],...]

    else:                                                # if type_of_ip == 'ipv6'
        for addr in arr_with_addr:
            arr: list[str] = addr.split(':')             # ['ffe0', '', '80', '0', '0', '0']

            count_zero_for_addr: int = 8 - len(arr) + 1  # count zero for '::' or ''
            zero_arr: list[str] = []
            for i in range(count_zero_for_addr):
                zero_arr += '0'                          # ['0', '0', '0']

            for i in range(len(arr)):                    # replacing '' with zero_arr
                if arr[i] == '':
                    arr[i:i:1] = zero_arr
                    arr.remove('')
                    addr_by_octets.append(arr)           # [['ffe0', '0', '0', '0', '1', '0', '0', '0' ], ...]
                    break

    return addr_by_octets                                # type: list[list[str]]


def check_correct_of_addr(addr: list, type_of_ip: str):           # for step 5, type of addr: list[list[str]]

    if type_of_ip == 'ipv4':
        for i in range(len(addr)):
            count_of_octets: int = len(addr[i])
            if count_of_octets != 4:                              # count of octets for IPv4 can to be only = 4
                return False
            for j in range(count_of_octets):
                if int(addr[i][j]) < 0 or int(addr[i][j]) > 255:  # value of octet can to be only '0 - 255'
                    return False

    else:                                                         # if type_of_ip == 'ipv6'
        for i in range(len(addr)):
            if len(addr[i]) != 8:                                 # count of hextets for IPv6 can to be only = 8
                return False

            for j in range(len(addr[i])):
                if not addr[i][j].isdigit():                      # if the element is hex form
                    num_from_hex: int = int(addr[i][j], 16)
                else:
                    num_from_hex: int = int(addr[i][j])

                if num_from_hex < 0 or num_from_hex > 65535:      # value of hextet can to be only '0 - 65535'
                    return False

    return True


def search_octet_with_host(addr: list):     # for step 6, type of addr: list[list[str]]
    for j in range(len(addr[0])):           # len(addresses[0]) == count of octets/hextets in IP-addresses
        for i in range(len(addr) - 1):
            if addr[i][j] != addr[i + 1][j]:
                return j                    # type: int

    return None


def search_max_num_in_octets(addr: list, octet_with_host: int):  # for step 7, type of addr: list[list[str]]
    max_num: int = int(addr[0][octet_with_host])
    for i in range(1, len(addr)):
        if max_num < int(addr[i][octet_with_host]):
            max_num = int(addr[i][octet_with_host])

    return max_num                                 # type: int


def get_mask(num: int, type_of_ip: str, octet_with_host: int):   # for step 8
    bin_num: str = str(bin(num))                  # for search max count bit for host
    bit_for_host: int = 0                         # it is counter of bit for host

    if type_of_ip == 'ipv4':
        bit_in_addr: int = 32
        bit_in_section: int = 8

    else:                                         # for 'ipv6'
        bit_in_addr: int = 128
        bit_in_section: int = 16

    arr_bin_num: list[str] = list(bin_num)

    for i in range(len(arr_bin_num) - 1, 0, -1):
        if arr_bin_num[i] == 'b':
            zero: int = bit_in_section - len(arr_bin_num)
            zero_line: list[str] = []

            for zer in range(zero + 1):           # because we have 'b' -> zero
                zero_line.append('0')

            arr_bin_num[i:i:1] = zero_line
            arr_bin_num.remove('b')
            break

        bit_for_host += 1                                  # if bit != 'b'

    bit_for_host += int(((bit_in_addr / bit_in_section) - octet_with_host - 1) * bit_in_section)

    if type_of_ip == 'ipv4':
        mask: str = '/' + str(bit_in_addr - bit_for_host)  # for 'ipv4' = 32 bits

    else:                                                  # if 'ipv6'
        bit_for_net = (bit_in_addr-bit_for_host) - (bit_in_addr-bit_for_host) % 4
        mask: str = '/' + str(bit_for_net)
        bit_for_host = bit_in_addr - bit_for_net

    return (mask, arr_bin_num, bit_for_host)                # type mask: str, bin_num: list[str], bit_for_host: int


def get_net_octet(num: list, bit_for_host: int, type_of_ip: str):  # for step 9, type of num: list[str]

    if type_of_ip == 'ipv4':
        for i in range(bit_for_host - 1, len(num)):
            num[i] = '0'

    else:                                          # if 'ipv6'
        bit_for_host_in_octet: int = 16 - bit_for_host % 16
        for i in range(bit_for_host_in_octet, len(num)):
            num[i] = '0'

    return str(int(''.join(num)))                 # type: str


def get_result(addr: list, net_octet: str, octet_with_host: int, mask: str, type_of_ip: str):  # for step 10
    buf: list[str] = []
    res: list[str] = []
    for i in range(octet_with_host):
        buf.append(addr[i])
    buf.append(net_octet)

    if type_of_ip == 'ipv4':
        res = '.'.join(buf)
        res += mask

    else:                    # if 'ipv6'
        flag: bool = False
        for i in range(len(buf)):
            if buf[i] == '0' and flag == False:
                res.append('::')
                flag = True
            elif buf[i] != '0':
                res.append(buf[i])

        res = ''.join(res)
        res += mask

    return res               # type: str


if __name__ == '__main__':
    start = time.perf_counter()
    print(search_subnet())
    print(f'The search_subnet() takes {round(time.perf_counter() - start, 9)} sec.')