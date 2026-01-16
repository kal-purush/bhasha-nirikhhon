try:                                    # import the important library
    from urllib import request
    import requests                     # import requests for web API
    import ssl
    import random
    import time

except:
    print("No Library Found")


class Thingspeak(object):                       # define a class called Thingspeak

    def __init__(self, write_api_key = None, read_api_key=None, channel_id=0):

        """

        :param write_key:  takes a string of write api key
        :param timer: can take integer values
        """

        # self.url = 'https://api.thingspeak.com/update?api_key='
        # self.read_url = 'https://api.thingspeak.com/channels/{}/feeds.json?api_key='.format(channel_id)

        self.write_key = write_api_key
        self.channel_id = channel_id
        self.read_api_key = read_api_key

        # Private Var cannot change
        self.__url = 'http://api.thingspeak.com/update?api_key'
        self.__read_url = 'https://api.thingspeak.com/channels/{}/feeds.json?api_key='.format(channel_id)


        self.feild1 = []
        self.feild2 = []
        self.feild3 = []

    def post_cloud(self, value1, value2, value3):
        try:
            """
            :param value1: can be interger or float
            :param value2: can be interger or float
            :return: updated to cloud storage
            """

            URL = self.__url

            KEY = self.write_key

            HEADER = '&field1={}&field2={}&field3={}'.format(str(value1), str(value2), str(value3))

            NEW_URL = str(URL) + "=" + str(KEY) + str(HEADER)
            # print(NEW_URL)

            context = ssl._create_unverified_context()

            data = request.urlopen(NEW_URL,context=context)
            # print(data)
        except:
            print('could not post to the cloud server ')

    def read_cloud(self, result=2):
        try:
            """
            :param result: how many data you want to fetch accept interger
            :return: Two List which contains Sensor data
            """

            URL_R = self.__read_url
            read_key = self.read_api_key
            header_r = '&results={}'.format(result)

            new_read_url = URL_R + read_key + header_r

            data = requests.get(new_read_url).json()

            field1 = data['feeds']

            for x in field1:
                self.feild1.append(x['field1'])
                self.feild2.append(x['field2'])
                self.feild3.append(x['field3'])


            return self.feild1, self.feild2, self.feild3
        except:
            print('read_cloud failed !!!! ')


w_key = 'F04QHPGCK4NWN7UX'
r_key = '2WI5DIAIWU76CLX5'
channel_id = 1827388                             # replace with channel id

ob = Thingspeak(write_api_key=w_key, read_api_key=r_key, channel_id=channel_id)

def sendData(temperature, bpm, spo2):
    '''
    Function for sending data to ThingSpeak
    Take three arguments; temperature, bpm, spo2
    '''
    ob.post_cloud(value1=temperature,value2=bpm,value3=spo2)
    time.sleep(5)

def readData(hasil=3):
    print(ob.read_cloud(hasil))                # change result=number of data you want

def randomData():
    '''
    Function to generate random data
    '''
    temperature = random.randint(35,40)
    bpm = random.randint(50,100)
    spo2 = random.randint(90,100)

    return temperature, bpm, spo2

def main():
    '''
    DO NOT import this function
    '''
    dta = randomData()
    print('Data to be send:')
    print(f'Temperature : {dta[0]}')
    print(f'BPM         : {dta[1]}')
    print(f'SPO2        : {dta[2]}')
    sendData(dta[0], dta[1], dta[2])
    print('Data sent!')

if __name__ == '__main__':
    main()