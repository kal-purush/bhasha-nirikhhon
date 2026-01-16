import requests
import time
from gpiozero import MCP3008
import json
import datetime
import mysql.connector
from datetime import datetime
import RPi.GPIO as GPIO
import time


#converts measured value into kg
def convert_wght(gen):
    for value in gen:
        yield 7.2-value*25


#check to see if scale is measuring
adc = MCP3008(channel=0)
i = 7
for temp in convert_wght(adc.values):
    print(temp)
    break

#returns the last measured value
def get():
    for wght in convert_wght(adc.values):

        return wght
        break


# for temp in convert_temp(adc.values):
#     print('The weight is', temp, 'kg')
#     temp2= int(temp)
#     temp2 = float(temp2)
#
#
#
#     i=i+1
#     time.sleep()


#preforms he weigh measurement and stores the value in the measurement table
def weight():
    cnx = mysql.connector.connect(host ="mysql.studev.groept.be", user="a21ib2a03", password="secret")
    cursor = cnx.cursor()

    query = ("INSERT INTO a21ib2a03.weight_measurements  (cat_id, date_and_time, weight)VALUES ( %s, %s, %s) ")

    now = datetime.now()
    cat=2
    #weight = convert_temp(adc.value)
    weight = get()
    print(weight)
    print("!")

    cursor.execute(query, (cat, now, weight))
    print("okb")
    cnx.commit()



    cursor.close()
    cnx.close()


#stores the id of the current profile of cat we are in (not useful yet so empty for now)
def save_cat():
    None



def feed_sequence():
    None



##code for motor
# pin numbering declaration
GPIO.setmode(GPIO.BCM)

# set pin 3 as an output for motor
GPIO.setup(2, GPIO.OUT)
# set pin 11 as input of Hallsensor
GPIO.setup(17, GPIO.IN)

# pin 3 is set low in the beginning
GPIO.output(2, GPIO.LOW)

pwm = GPIO.PWM(2,1000)  # set pin 2 as a PWMpin and frequenty is 1000 Hz
pwm.start(0)
# 0 is dutycycle in the start (dc has to be between 0 and 100 (0% - 100%))


def switchCounter(push_count):
    cur_status = False

    # scan GPIO11 LOW or HI every 0.1sec,
    status = GPIO.input(17)

    # check change status
    if cur_status != status:
        if status == True:
            print ('switch on')
            push_count += 1
        else:
            print ('switch off')
        cur_status = status

        # wait 0.5sec
    time.sleep(0.6)
    return push_count


def turnMotor(feed, avWeight,Weight, average, section, count):
    feed=True
    push_count=0
    if feed:
        if avWeight > Weight:
            fraction = Weight / avWeight
        elif avWeight < Weight:
            fraction = -(avWeight / Weight)
        elif Weight == avWeight:
            fraction = 0
        # hall sensor timing for amount of food (formulae for food ifo weight)
        amount = average + fraction
        print(amount)
        if amount < 0.5:
            amount = 0.5
        #hallcounter = amount/section
        hallcounter =5
        cnt = count
        while push_count < hallcounter:
            push_count = switchCounter(push_count)
            pwm.ChangeDutyCycle(75.0)     # change dc to 75%
            GPIO.output(2, GPIO.HIGH)
            print("motor is turning")
            time.sleep(5)

        feed = False
        GPIO.output(2, GPIO.LOW)

    else:
        GPIO.output(2, GPIO.HIGH)
        print("motor isn't turning")
        #time.sleep(1)
    GPIO.output(2, GPIO.HIGH)



def wantToFeed(feed):
    if feed:
        return True
    else:
        return False





#define status and its defualt value
status =0


#this sequence alway happens and checks the log_table for changes in the status
while True:

    #status = query to get last status value
    cnx = mysql.connector.connect(host ="mysql.studev.groept.be", user="a21ib2a03", password="secret")
    cursor = cnx.cursor()

    query = ("select * from a21ib2a03.log_table ORDER BY log_id DESC LIMIT 1")

    now = datetime.now()

    #weight = convert_temp(adc.value)



    cursor.execute(query)
    records = cursor.fetchall()

    for row in records:
        status = row[1]
        cat_id = row[4]
        feed = row[5]
    print("ok")
    cnx.commit()



    cursor.close()
    cnx.close()

    if(status == 303):
        p=weight()

        #json that adds line with status = 0
        cnx = mysql.connector.connect(host ="mysql.studev.groept.be", user="a21ib2a03", password="secret")
        cursor = cnx.cursor()

        query = ("INSERT INTO a21ib2a03.log_table  (log_code,time) VALUES (%s,%s) ")

        now = datetime.now()
        i = 0

        #weight = convert_temp(adc.value)



        cursor.execute(query, (i, now))
        print("ok weigh")
        cnx.commit()



        cursor.close()
        cnx.close()
        status =0
        time.sleep(3)


        feed_sequence()
        #json that adds line with status = 0
        cnx = mysql.connector.connect(host ="mysql.studev.groept.be", user="a21ib2a03", password="secret")
        cursor = cnx.cursor()

        query = ("INSERT INTO a21ib2a03.log_table  (log_code, time)VALUES ( %s, %s) ")
        print(cat_id)
        query2 = ("SELECT * FROM a21ib2a03.cat_table WHERE idcat="+str(cat_id))



        i = 0
        now = datetime.now()





        cursor.execute(query, (i,now))
        cnx.commit()




        cursor.execute(query2)
        frou = cursor.fetchall()
        for t in frou:
            ideal_weight = t[4]

        now = datetime.now()
        print("ok feed")



        cnx.commit()

        query3= ("select * from a21ib2a03.weight_measurements ORDER BY idw DESC LIMIT 1")
        cursor.execute(query3)
        u = cursor.fetchall()
        for slee in u:
            actual_weight = slee[3]



        cursor.close()
        cnx.close()
        status = 0

        #code for motor
        i = turnMotor(True, ideal_weight, actual_weight,1,0.05, 0)
        GPIO.output(2, GPIO.HIGH)
        pwm.ChangeDutyCycle(0.0)




    time.sleep(2)

    # elif(status == 202):
    #     save_cat()
    #     #json that adds line with status = 0
    #     cnx = mysql.connector.connect(host ="mysql.studev.groept.be", user="a21ib2a03", password="secret")
    #     cursor = cnx.cursor()
    #
    #     query = ("INSERT INTO a21ib2a03.log_table  (log_code, time)VALUES ( 0, %s) ")
    #
    #     now = datetime.now()
    #
    #     #weight = convert_temp(adc.value)
    #
    #
    #
    #     cursor.execute(query, now)
    #     print("ok save")
    #     cnx.commit()
    #
    #
    #
    #     cursor.close()
    #     cnx.close()
    #     status = 0

    # elif(status == 505):
    #     feed_sequence()
    #     #json that adds line with status = 0
    #     cnx = mysql.connector.connect(host ="mysql.studev.groept.be", user="a21ib2a03", password="secret")
    #     cursor = cnx.cursor()
    #
    #     query = ("INSERT INTO a21ib2a03.log_table  (log_code, time)VALUES ( %s, %s) ")
    #     query2 = ("SELECT * FROM a21ib2a03.cat_table WHERE idcat="+str(cat_id))
    #
    #
    #
    #     i = 0
    #     now = datetime.now()
    #
    #
    #
    #
    #
    #     cursor.execute(query, (i,now))
    #     cnx.commit()
    #
    #
    #
    #
    #     cursor.execute(query2)
    #     frou = cursor.fetchall()
    #     for t in frou:
    #         ideal_weight = t[4]
    #
    #     now = datetime.now()
    #     print("ok feed")
    #
    #
    #
    #     cnx.commit()
    #
    #     query3= ("select * from a21ib2a03.log_table ORDER BY log_id DESC LIMIT 1")
    #     cursor.execute(query3)
    #     u = cursor.fetchall()
    #     for slee in u:
    #         actual_weight = slee[3]
    #
    #
    #
    #     cursor.close()
    #     cnx.close()
    #     status = 0
    #
    #     #code for motor
    #     i = turnMotor(True, ideal_weight, actual_weight,1,0.05, 0)
    #
    #
    #
    #
    # time.sleep(2)