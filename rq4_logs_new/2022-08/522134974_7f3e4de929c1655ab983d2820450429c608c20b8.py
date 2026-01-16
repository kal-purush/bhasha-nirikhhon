
# COVID-19 Statistics Notification and Dashboard System

"""
    A Python Project by:
        20BT04006: Avanish Deshpande
        20BT04045: Devendra Sharma
        20BT04051: Varada Deshpande

    Students, 2nd year (Semester 3),
    B.Tech. Computer Science and Engineering,
    GSFC University, Vadodara.
    (Academic Year: 2021-22)
"""





""" Importing the Required Libraries """

# plyer: for desktop notifications
from plyer import notification


# For Web Scraping

# requests: to extract the required data from the website(s)
import requests

# bs4.BeautifulSoup: HTML Parsing
from bs4 import BeautifulSoup



# matplotlib: to plot graphs of data, and embed them into the tkinter window
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)


# tkinter: to create a dashboard with the relevant data
from tkinter import *
from tkinter import ttk
import tkinter as tk





""" class covidData to create objects having state-wise covid information """

class covidData:
    def __init__(self, state, total, recovered, death, active):
        self.state = state
        self.total = total
        self.recovered = recovered
        self.death = death
        self.active = active


    @staticmethod
    def createTable(treev, data):
        for i in data.values():
            e = treev.insert("", 'end', text ="L1", values =(i.state, i.total, i.recovered, i.death, i.active))


    @staticmethod
    def showGraph(states, stats, chartTitle, color):
    	# Create tkinter window
        g = tk.Tk()
        g.title(chartTitle)
        g.resizable(width = 850, height = 1000)

        fig = Figure(figsize = (15, 10), dpi = 100)
        a = fig.add_subplot(111)
        rects1 = a.barh(states, stats, 0.5, color=color)
        fig.suptitle(chartTitle, fontweight ="bold")

        canvas = FigureCanvasTkAgg(fig, master=g)
        canvas.draw()
        canvas.get_tk_widget().pack()

        # creating the Matplotlib toolbar
        toolbar = NavigationToolbar2Tk(canvas, g)
        toolbar.update()
        canvas.get_tk_widget().pack()
        g.mainloop()
    

            
            

def notifyMe(title="Let us beat this virus together!", message="A Python assignment by:\n20BT04006: Avanish Deshpande\n20BT04045: Devendra Sharma\n20BT04051: Varada Deshpande", icon = "Logo.ico", Covid19Data=None):
    if Covid19Data!=None:
        r = requests.get('https://ipinfo.io/')
        data = r.json()
        print("\n\nUser Data Collected:\n\n", data)
        region = data['region']
        title = "\nState: " + Covid19Data[region].state
        message = "Confirmed Cases: " + str(Covid19Data[region].total) + "\nRecovered Cases: " + str(Covid19Data[region].recovered) + "\nDeaths: " + str(Covid19Data[region].death) + "\nActive Cases: " + str(Covid19Data[region].active)
    
    # Creating Desktop Notification using the plyer library (notification module)
    notification.notify(
        title = title,
        message = message,
        app_icon = icon,
        timeout = 10
    )
    
    
    
    
    
    
""" THE 'main' METHOD """
 
if __name__ == "__main__":
   
    """ Extract data from url """
    myHtmlData = requests.get('http://bioinfo.usu.edu/covidTracker/india.php').text
    soup = BeautifulSoup(myHtmlData, 'html.parser')
    
    # Extracting the required data from the table in the website
    myDataStr = []
    for td in soup.find_all('td'):
        myDataStr.append(td.get_text())
        
    print("\nmyDataStr: data extracted from the table:\n\n", myDataStr, "\n\n")




    """
        Creating objects of type "covidData" using the extracted data, and storing them in a dictionary with name of the state as key.
        Separate lists are for storing statistical data: to help plot graphs.
    """
    data = {} # Dictionary for covidData objects
    states = []
    totalCases = []
    recoveredCases = []
    deaths = []
    activeCases = []

    for i in range(0, len(myDataStr), 5):
        state = myDataStr[i]
        if (state!="State Unassigned"):
            total = int(myDataStr[i+1])
            recovered = int(myDataStr[i+2])
            death = int(myDataStr[i+3])
            active = int(myDataStr[i+4])
            data[state] = covidData(state, total, recovered, death, active)
            states.append(state)
            totalCases.append(total)
            recoveredCases.append(recovered)
            deaths.append(death)
            activeCases.append(active)
            
            
            
    
    """ Desktop Notification """
    # Notification 1: Python Assignment members
    notifyMe()
    # Notification 2: Covid 19 Data (Confirmed, Recovered, Active cases and deaths): Data displayed according to the current state of residence of user
    notifyMe(Covid19Data=data)
    
    
    
    """ Tkinter Window """
    # Create tkinter window, specify its size
    window = tk.Tk()
    # window.resizable(width = 1500, height = 1000)
    window.geometry("1500x1000+0+0") #window.geometry("window width x window height + position right + position down")

    # Creating object of photoimage class: Image should be in the same folder in which script is saved
    p = PhotoImage(file = 'logo.png')

    # Setting icon of tkinter window
    window.iconphoto(False, p)

    # Setting window title
    window.title("COVID-19 Statistics for Indian States and Union Territories")


    img = Label(window, image=p, width=1500)
    l = Label(window, text="COVID-19 Statistics for Indian States and Union Territories", font=(None, 22, 'bold'))
    img.pack(pady=5)
    l.pack(pady=30)


    
    """ Tkinter Frame """
    # Creating frame within tkinter window
    frame = Frame(window)


    
    """ Treeview """
    # Creating a Treeview within the frame, and setting its style
    treev = ttk.Treeview(frame, selectmode ='browse', height=15)
    ttk.Style().configure("Treeview", background="black", foreground="white", fieldbackground="black", font=(None, 14), rowheight=35)
    ttk.Style().configure("Treeview.Heading", font=(None, 17, 'bold'))
    
    # Calling pack method with respect to treeview
    treev.pack(side="top")
    
    # Defining the number of columns in treeview
    treev["columns"] = ("1", "2", "3", "4", "5")
 
    # Defining headings for treeview
    treev['show'] = 'headings'
 
 
    # Assigning the width and anchor to the respective columns
    treev.column("1", width = 400, anchor ='w')
    treev.column("2", width = 200, anchor ='c')
    treev.column("3", width = 220, anchor ='c')
    treev.column("4", width = 180, anchor ='c')
    treev.column("5", width = 170, anchor ='c')
    
    
    # Assigning the heading names to the respective columns
    treev.heading("1", text ="State")
    treev.heading("2", text ="Confirmed Cases")
    treev.heading("3", text ="Recovered Cases")
    treev.heading("4", text ="Deaths")
    treev.heading("5", text ="Active Cases")
    
    
    # Inserting the items and their features to the columns built
    t = covidData.createTable(treev, data)
    


    """ Ploting graphs, and displaying them to the tkinter window """

    btnFrame = Frame(window)

    # Create Buttons, add action listeners to the buttons, and "pack" them to a frame
    # Action Listener: on clicking a button, open a new tkinter window containing the graph for the requested statistics

    btn1 = Button(btnFrame, text = 'View Graph for Total COVID-19 Confirmed Cases', bd = '5', bg='red', font='sans 11 bold')
    btn1.bind("<Button>", lambda e: covidData.showGraph(states, totalCases, 'State-wise Total COVID-19 Confirmed Cases', 'red'))
    btn1.pack(side='left', padx=10)

    btn2 = Button(btnFrame, text = 'View Graph for Total COVID-19 Recovered Cases', bd = '5', bg='green', font='sans 11 bold')
    btn2.bind("<Button>", lambda e: covidData.showGraph(states, recoveredCases, 'State-wise Total COVID-19 Recovered Cases', 'green'))
    btn2.pack(side='right', padx=10)

    btn3 = Button(btnFrame, text = 'View Graph for COVID-19 Active Cases', bd = '5', bg='lightblue', font='sans 11 bold')
    btn3.bind("<Button>", lambda e: covidData.showGraph(states, activeCases, 'State-wise COVID-19 Active Cases', 'blue'))
    btn3.pack(side='left', padx=10)

    btn4 = Button(btnFrame, text = 'View Graph for Total COVID-19 Deaths', bd = '5', bg='lightgrey', font='sans 11 bold')
    btn4.bind("<Button>", lambda e: covidData.showGraph(states, deaths, 'State-wise COVID-19 Deaths', 'grey'))
    btn4.pack(side='right', padx=10)

    # Pack the buttons-containing frame to the tkinter window
    btnFrame.pack(side='bottom', pady=50)
    

    
    """ Calling pack with respect to frame """
    frame.pack(pady=30)
    
    
    
    """ Calling mainloop """
    window.mainloop()
    
    
    
    """ Desktop Notification: Displayed on closing the tkinter window """
    notifyMe("Thank You!", "We are grateful for your support and guidance!\nMs. Rujul Desai\nMs. Zalak Kansagra", 'thankYou.ico')
    
    