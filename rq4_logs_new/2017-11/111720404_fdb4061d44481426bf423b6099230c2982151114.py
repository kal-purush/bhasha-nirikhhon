import socket
import datetime

sniffed_socket = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)

data_size = []
data_size.append(0)
operator = 0

current_second = datetime.datetime.now().second
while True:
    packet = sniffed_socket.recvfrom(8080)[0]

    result_file = open('analysis_result.txt', 'w')
    
    if datetime.datetime.now().second == current_second:
        data_size[operator] += len(packet)
    else:
        current_second = datetime.datetime.now().second    
        operator += 1
        data_size.append(0)
    
    for each in data_size:
        result_file.write(str(each) + 'bytes' + '\n')

    result_file.write('\nAverage: ' + str(sum(data_size)/len(data_size)) + 'bytes/sec\n')

    result_file.close()
    