'''
View the coding live on Twitch @ https://www.twitch.tv/gmangavin and look at the github @ https://github.com/gmangavin/PyWeb
chromedriver for gui view, phantomjs for ghost view.
'''

import selenium.webdriver #Imports module
import time #Imports time
import threading #Imports threading, used to have multiple things happen at the same time.
import os #Imports OS

N = False #Used for the bool loop.
while N == False:
    EngineChoice = input('Would you like a visual of the bot? (Y/N): ') #Part one for the web driver choice
    YN = (EngineChoice.lower()) #Prevents capatalization error.
    if YN == ('y'):
        while N == False:
            VarChoice = input('Would you like Firefox or Chrome? (F/C): ') #Part two for the web driver choice
            FC = (VarChoice.lower()) #Prevents capatalization error.
            if FC == ('f'):
                try:
                    WebVar = selenium.webdriver.Firefox()
                    N = True
                except selenium.common.exceptions.WebDriverException:
                    print("You don't seem to have the firefox webdriver installed. You can get it here https://github.com/mozilla/geckodriver/releases")
            elif FC == ('c'):
                try:
                    WebVar = selenium.webdriver.Chrome()
                    N = True
                except:
                    print("You don't seem to have the chrome webdriver installed. You can get it here https://sites.google.com/a/chromium.org/chromedriver/downloads")
            else:
                print('Try again')
    elif YN == ('n'):
        try:
            WebVar = selenium.webdriver.PhantomJS()
            N = True
        except selenium.common.exceptions.WebDriverException:
                    print("You don't seem to have the PhantomJS webdriver installed. You can get it here http://phantomjs.org/")
    else:
        print('Try again')
#A while loop to make sure the user enters in a correct character.
#Allows the user to choose which web driver they want to use.

Interest = input("What is a common interest you're looking for?: ")
WebVar.get('https://www.omegle.com')
print(WebVar.title)

WebVar.find_element_by_xpath('//*[@id="topicsettingscontainer"]/div/div[1]/span[2]').click() #Clicks the area for typing
Send = WebVar.find_element_by_class_name('newtopicinput') #Creates an input variable for text area.
Send.send_keys(Interest + ',') #Sends input to text area.

WebVar.find_element_by_xpath('//*[@id="textbtn"]').click() #Clicks the 'text' button

Disconnected = False

def Disconnect(*args):
    WebVar.find_element_by_xpath('/html/body/div[7]/div/div/div[2]/table/tbody/tr/td[1]/div/button').click()
    WebVar.find_element_by_xpath('/html/body/div[7]/div/div/div[2]/table/tbody/tr/td[1]/div/button').click()
    global Disconnected
    Disconnected = True
    return Disconnected

def Change(*args):
    if Disconnected == True:
        os.system('cls')
        WebVar.find_element_by_xpath('/html/body/div[7]/div/div/div[1]/div[1]/div/div[4]/div/a').click()
        Interest = input("What is a common interest you're looking for?: ")
        Send2 = WebVar.find_element_by_class_name('topicplaceholder')
        Send2.send_keys(Interest + ',') #Sends input to text area.
    else:
        print("You need to disconnect first")

def Connect(*args):
    if Disconnected == True:
        os.system('cls')
        WebVar.find_element_by_xpath('/html/body/div[7]/div/div/div[2]/table/tbody/tr/td[1]/div/button').click()
        os.system('cls')
        print('Rebooting search.')
        #threading.Thread(target=StatusMode)._stop()
        #threading.Thread(target=UserMode)._stop()
        time.sleep(1)
        #threading.Thread(target=StatusMode).start()
        os.system('cls')
    elif Disconnected == False:
        print("You're still connected.")
    else:
        print("something is just broken")

def UserMode(*args):
    while True:
        UserM = input('') #Has the user type an interest.
        if UserM == "/end":
            Disconnect()
        elif UserM == "/start":
            Connect()
        elif UserM == "/change":
            Change()
        else:
            Sending = WebVar.find_element_by_class_name('chatmsg') #Takes the class used for user input.
            Sending.send_keys(UserM)
            WebVar.find_element_by_class_name('sendbtn').click()

def StatusMode(*args):
    threading.Thread(target=UserMode).start() #Starts the operation in a thread.
    StatusNew = None #Create a variable with no value.
    while True:
        Status = WebVar.find_element_by_xpath('/html/body/div[7]/div/div/div[1]/div[1]/div').text #Takes the text info from xpath
        if StatusNew == (Status):
            continue
        else:
            StatusNew = Status
            if "Stranger has disconnected." not in Status:
                os.system('cls') #Refreshes chat.
                print(StatusNew)
                print('')
            else:
                Disconnect()

threading.Thread(target=StatusMode).start() #Starts the operation in a thread.