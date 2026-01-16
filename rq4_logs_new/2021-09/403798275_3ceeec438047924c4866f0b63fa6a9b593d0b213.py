import math
import csv
import tempfile
import time
start = time.time()

data_list = [
['1984','George Orwell','978-123',268],
['Animal Farm','George Orwell','978-056',144],
['Brave New world','Aldous Huxley','978-006',288]
]

def function1():
    for i in range(1, 101):
        if i % 3 == 0:
            print('Fizz')
        elif i % 5 == 0:
            print('Buzz')
        elif i % 3 == 0 and i % 5 == 0:
            print('FizzBuzz')
        else:
            print(i)
    print(time.time() - start, 'seconds')

def function2(num):
    volume = 4/3*math.pi*num**3
    print(volume)

def function3(datalist):
    with open('book.csv','w',newline='') as csvfile:
        writer = csv.writer(csvfile)
        fieldnames=['Title','Author','ISBN13','Pages'] 
        writer.writerow(fieldnames)

        for row in datalist:
            writer.writerow(row)
    csvfile.close()

def function4():
    with open('book.csv','r',newline='') as book:
        reader = csv.reader(book)
        for row in reader:
            print(', '.join(row))
    book.close()

def function5(data):
    temp = tempfile.NamedTemporaryFile(delete=False)
    fieldnames=['Title','Author','ISBN13','Pages'] 
    dictionary={}
    try:
        with open(temp.name,'w',newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)
            for row in data:
                try:
                    writer.writerow(row)
                except Exception as error:
                    print('Error in writing row:',error)
        with open(temp.name,'r',newline='') as book:
            reader = csv.reader(book)
            for row in reader:
                print(', '.join(row))

    finally:
        temp.close()
    print(dictionary)

function1()
function2(3)
function3(data_list)
function4()
function5(data_list)
            