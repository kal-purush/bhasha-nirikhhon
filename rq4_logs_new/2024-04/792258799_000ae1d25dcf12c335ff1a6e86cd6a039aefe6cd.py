#By John Danias for Rasberry Pi Pico (one thread version)

import network
import socket
from time import sleep
from machine import ADC, RTC
import machine
import rp2
import sys
import utime as time
import ustruct as struct
import json
import os

ssid = 'Test'
password = 'HelloWorld'


temp_pin = 26
tmp36 = ADC(temp_pin)

MAX_DAYS_HISTORY = 10

filename_data = 'data.json'
filename_max = 'max_data.json'
filename_average = 'average_data.json'

# try:
#     stat_info = os.stat(filename_data)
#     os.remove(filename_data)
# except OSError:
#     pass
try:
    stat_info = os.stat(filename_max)
    os.remove(filename_max)
except OSError:
    pass
try:
    stat_info = os.stat(filename_average)
    os.remove(filename_average)
except OSError:
    pass

file_data = open(filename_data, 'w')
file_max = open(filename_max, 'w')
file_average = open(filename_average, 'w')

initial_data = {
    "year": 0,
    "month": 0,
    "day": 0,
    "hour": 0,
    "minute": 0,
    "second": 0,
    "temperature": 0
}
file_data.write(json.dumps(initial_data) + "\n")
print(file_data.readlines())
# wintertime / Summerzeit
#GMT_OFFSET = 3600 * 1 # 3600 = 1 h (wintertime)
GMT_OFFSET = 3600 * 2 # 3600 = 1 h (summertime)

# NTP-Host
NTP_HOST = 'pool.ntp.org'


# Funktion: get time from NTP Server
def getTimeNTP():
    NTP_DELTA = 2208988800
    NTP_QUERY = bytearray(48)
    NTP_QUERY[0] = 0x1B
    addr = socket.getaddrinfo(NTP_HOST, 123)[0][-1]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.settimeout(5)
        res = s.sendto(NTP_QUERY, addr)
        msg = s.recv(48)
    finally:
        s.close()
    ntp_time = struct.unpack("!I", msg[40:44])[0]
    return time.gmtime(ntp_time - NTP_DELTA + GMT_OFFSET)


# Funktion: copy time to PI pico´s RTC
def setTimeRTC():
    tm = getTimeNTP()
    rtc.datetime((tm[0], tm[1], tm[2], tm[6] + 1, tm[3]+1, tm[4], tm[5], 0))


def connect():
    #Connect to WLAN
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(ssid, password)
    while wlan.isconnected() == False:
        print('Waiting for connection...')
        sleep(1)
    ip = wlan.ifconfig()[0]
#     ip = wlan.ifconfig(('192.168.137.148', '255.255.255.0', '192.168.0.1', '8.8.8.8'))
    print(f'Connected on {ip}')
    return ip


def open_socket(ip):
    #Open a socket
    address = (ip, 80)
    connection = socket.socket()
    connection.bind(address)
    connection.listen(1)
    return connection


def read_html_file(filename):
    with open(filename, 'r') as file:
        return file.read()


def webpage(temperature, time, max_temp, max_temp_time, average_temp):
    html = read_html_file('index.html')
    html = html.replace('{temperature}', str(temperature))
    html = html.replace('{time[2]}', str(time[2]))
    html = html.replace('{time[1]}', str(time[1]))
    html = html.replace('{time[0]}', str(time[0]))
    html = html.replace('{time[4]}', str(time[4]))
    html = html.replace('{time[5]}', str(time[5]))
    html = html.replace('{time[6]}', str(time[6]))
    html = html.replace('{max_temp}', str(max_temp))
    html = html.replace('{max_temp_time[0]}', str(max_temp_time[0]))
    html = html.replace('{max_temp_time[1]}', str(max_temp_time[1]))
    html = html.replace('{max_temp_time[2]}', str(max_temp_time[2]))
    html = html.replace('{average_temp}', str(average_temp))

    return html


def history_page():
    table_rows = ""

    with open(filename_max, 'r') as file_max, open(filename_average, 'r') as file_average:
        max_lines = file_max.readlines()
        average_lines = file_average.readlines()

        for max_line, average_line in zip(reversed(max_lines), reversed(average_lines)):
            max_data = json.loads(max_line)
            average_data = json.loads(average_line)

            table_rows += f"<tr><th>{max_data['date']['day']}/{max_data['date']['month']}/{max_data['date']['year']}</th>"
            table_rows += f"<td>{max_data['max_temperature']}</td>"
            table_rows += f"<td>{average_data['average_temperature']}</td></tr>"

    html = read_html_file('history.html')
    html = html.replace('{history_data}', table_rows)
    
    return html


def ReadTemperature(tmp_pin):
    adc_value = tmp_pin.read_u16()
    volt = (3.3/65535)*adc_value
    temperature_tmp = (100*volt)-50
    degC = round(temperature_tmp,1)
    return degC


def WriteData(x,t,filename_data):
    current_date = (t[0], t[1], t[2])
    filename_data = filename_data
    try:
        with open(filename_data, 'r') as file:
            last_line = file.readlines()[-1]
            last_data = json.loads(last_line)
            last_recorded_date = (last_data["year"], last_data["month"], last_data["day"])

        if last_recorded_date != current_date:
            # If it's a new day, clear the data file
            os.remove(filename_data)
    except OSError as e:
        if e.args[0] == 2:  # 2 corresponds to file not found error
            pass
        else:
            raise e


    data = {
        "year": t[0],
        "month": t[1],
        "day": t[2],
        "hour": t[4],
        "minute": t[5],
        "second": t[6],
        "temperature": x
    }
    json_data = json.dumps(data)
    with open(filename_data, 'a') as file_data:
        file_data.write(json_data + '\n')


def WriteMaxData(max_temperature, max_temperature_time):
    data = {
        "max_temperature": max_temperature,
        "date": {
            "year": max_temperature_time[0],
            "month": max_temperature_time[1],
            "day": max_temperature_time[2]
        }
    }
    json_data = json.dumps(data)
    with open(filename_max, 'r') as file_max:
        lines = file_max.readlines()
    with open(filename_max, 'w') as file_max:
        found = False
        for i, line in enumerate(lines):
            existing_data = json.loads(line)
            existing_date = (existing_data["date"]["year"], existing_data["date"]["month"], existing_data["date"]["day"])
            if existing_date == (max_temperature_time[0], max_temperature_time[1], max_temperature_time[2]):
                lines[i] = json_data + '\n'
                found = True
        if not found:
            lines.append(json_data + '\n')
        # Trim the list to the last MAX_VALUES_LIMIT elements
        lines = lines[-MAX_DAYS_HISTORY:]
        for line in lines:
            file_max.write(line)
            
            
def WriteAverageData(average_temperature, current_date):
    data = {
        "average_temperature": average_temperature,
        "date": {
            "year": current_date[0],
            "month": current_date[1],
            "day": current_date[2]
        }
    }
    json_data = json.dumps(data)
    with open(filename_average, 'r') as file_average:
        lines = file_average.readlines()
    with open(filename_average, 'w') as file_average:
        found = False
        for i, line in enumerate(lines):
            existing_data = json.loads(line)
            existing_date = (existing_data["date"]["year"], existing_data["date"]["month"], existing_data["date"]["day"])
            if existing_date == (current_date[0], current_date[1], current_date[2]):
                lines[i] = json_data + '\n'
                found = True
        if not found:
            lines.append(json_data + '\n')
        # Trim the list to the last AVERAGE_VALUES_LIMIT elements
        lines = lines[-MAX_DAYS_HISTORY:]
        for line in lines:
            file_average.write(line)
            
            
def findMaxTemperature(filename,current_date):
    max_temperature = None
    with open(filename, 'r') as file:
        for line in file:
            data = json.loads(line)
            temperature = data.get("temperature")
            if temperature is not None:
                data_date = (data.get("year"), data.get("month"), data.get("day"))
                if data_date == current_date:
                    if max_temperature is None or temperature > max_temperature:
                        max_temperature = temperature
                        max_temperature_time = (data.get("hour"), data.get("minute"), data.get("second"))
    WriteMaxData(max_temperature, current_date)
    return max_temperature, max_temperature_time


def findAverageTemperature(filename, current_date):
    total_temperature = 0
    count = 0
    with open(filename, 'r') as file:
        for line in file:
            data = json.loads(line)
            temperature = data.get("temperature")
            if temperature is not None:
                data_date = (data.get("year"), data.get("month"), data.get("day"))
                if data_date == current_date:
                    total_temperature += temperature
                    count += 1
    if count > 0:
        average_temperature = total_temperature / count
    else:
        average_temperature = None
        
    average_temperature = round(average_temperature,2)
    WriteAverageData(average_temperature, current_date)
    return average_temperature


def serve(connection):
    temperature = 0
    state = '0'
    while True:
        client = connection.accept()[0]
        request = client.recv(1024)
        request = str(request)
        try:
            request = request.split()[1]
        except IndexError:
            pass
        temperature = ReadTemperature(tmp36)
        time = rtc.datetime()
        WriteData(temperature, time, filename_data)
        current_date = (time[0], time[1], time[2])
        
        max_temp, max_temp_time = findMaxTemperature('data.json',current_date)
        average_temp = findAverageTemperature('data.json', current_date)
            
        if request == '/history?':
            html = history_page()
        else:
            html = webpage(temperature, time, max_temp, max_temp_time, average_temp)
        
        client.send(html)
        client.close()


try:
    ip = connect()
    connection = open_socket(ip)
    rtc = RTC()  
    setTimeRTC()
    time = rtc.datetime()
    print(f'start at time {time}')
    serve(connection)
    
except KeyboardInterrupt:
    machine.reset()