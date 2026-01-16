#!/usr/bin/python
# -*- coding: <UTF-8> -*-
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
import os
import tkinter as tk
from PIL import ImageTk, Image
"""
Created on Sat Mar 21 20:53:24 2020
@authors: Andrzej Krzyżanowski (shaidel@wp.pl) – PM, visionary, backend
          Bartosz Sadocha – tester 
          Bartosz Sokół – GUI, backend helper
          Piotr Wereszczyński - refactoring, speed improvements
"""

"""
MIT LicenseCopyright (c) [2020] [Andrzej Krzyżanowski]
Permission is hereby granted, free of charge, to any person obtaining acopyof 
this software and associated documentation files (the "Software"),to dealin the Software 
without restriction, including without limitation therightsto use, copy, modify, merge, publish,
distribute, sublicense, and/orsellcopies of the Software, and to permit persons to whom 
the Software isfurnished to do so, subject to the following conditions:The above copyright 
notice and this permission notice shall be includedin allcopies or substantial portions of 
the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESSORIMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OFMERCHANTABILITY,FITNESS FOR A PARTICULAR 
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALLTHEAUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR 
ANY CLAIM, DAMAGES OR OTHERLIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
ARISINGFROM,OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN 
THE SOFTWARE.
"""

register_matplotlib_converters()
plt.rcParams['figure.figsize'] = [15, 5]
values = "bg"
links = {"Confirmed": 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data'
                      '/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
         "Deaths": 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data'
                   '/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
         "Recoveries": 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data'
                       '/csse_covid_19_time_series/time_series_covid19_recovered_global.csv '
         }

if not os.path.exists("images"):
    os.mkdir("images")


def cleandata(df_raw):
    df_cleaned = df_raw.melt(id_vars=['Country/Region'], value_name='Sick', var_name='Date')
    df_cleaned = df_cleaned.set_index(['Country/Region', 'Date'])
    return df_cleaned


def retrieve_data(data_type_name, region):
    certain_data = pd.read_csv(links[data_type_name])
    certain_data = cleandata(certain_data)
    return certain_data.loc[region].Sick.values[3:]


def retrieve_index(region):
    certain_data = pd.read_csv(links["Deaths"])
    certain_data = cleandata(certain_data)
    return certain_data.loc[region].index[3:]

def convert_data_to_daily_cases(all_data):
    converted_data = dict()
    for key in all_data:
        daily_cases = [0]
        data_entry = all_data[key]
        for i in range(1, len(data_entry)):
            daily_cases.append(abs(data_entry[i] - data_entry[i-1]))

        converted_data[key] = daily_cases
    return converted_data


def SUBMIT():
    values = name.get()
    all_data = dict()
    try:
        if var_deaths.get() == 1:
            all_data['Deaths'] = retrieve_data("Deaths", values)
        if var_recoveries.get() == 1:
            all_data['Recoveries'] = retrieve_data("Recoveries", values)
        if var_confirmed.get() == 1:
            all_data['Confirmed'] = retrieve_data("Confirmed", values)
    except KeyError:
        tk.messagebox.showerror(title='Error', message="There's not country like: '"+values+"'!")
        return

    if len(all_data) == 0:
        tk.messagebox.showerror(title='Error', message='No selected data!')
        return

    if var_daily.get() == 1:
        daily_cases = convert_data_to_daily_cases(all_data)
        index_values = retrieve_index(values)
        less_labels = []
        for i in range(0, len(index_values)):
            if i % 5 == 0:
                less_labels.append(index_values[i])
            else:
                less_labels.append('')
        # df = pd.DataFrame(, index=index_values)
        # df_sum = df.sum(numeric_only=True)
        ax = plt.axes()
        ax.xaxis.set_minor_locator(plt.MaxNLocator(len(index_values)/10))
        plt.xticks(range(0, len(index_values)), less_labels, rotation=45)

        

        if var_deaths.get() == 1:
            plt.bar(index_values, daily_cases["Deaths"])
        if var_recoveries.get() == 1:
            plt.bar(index_values, daily_cases["Recoveries"])
        if var_confirmed.get() == 1:
            plt.bar(index_values, daily_cases["Confirmed"])
            
        df = pd.DataFrame(daily_cases, index=retrieve_index(values))
        df.plot(figsize=(6, 4), title='COVID-19 in ' + values)   
        
       

    else:
        df = pd.DataFrame(all_data, index=retrieve_index(values))
        df.plot(figsize=(6, 4), title='COVID-19 in ' + values)

    plt.savefig("images/chart.png")
    img2 = ImageTk.PhotoImage(Image.open("images/chart.png"))
    panel.configure(image=img2)
    panel.image = img2
    plt.clf()


window = tk.Tk() 
width_of_window = 600
height_of_window = 600
screen_widht = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()
x_coordinate = (screen_widht/2) - (width_of_window/2)
y_coordinate = (screen_height/2) - (height_of_window/2)
window.title( "Check the COVID-19" )
window.geometry("%dx%d+%d+%d" % (width_of_window, height_of_window, x_coordinate, y_coordinate)) 




img = ImageTk.PhotoImage(Image.open("images/initial.png"))
panel = tk.Label(window, image=img)
panel.pack(side="bottom", fill="both", expand="yes")
    

text = tk.StringVar()
label = tk.Label(window, textvariable=text)
label.pack()

description = tk.Label(window, text="Please enter your country/province/state/region").pack()
name = tk.Entry(window, width=30)
name.pack()

var_confirmed = tk.IntVar()
var_deaths = tk.IntVar()
var_recoveries = tk.IntVar()
var_daily = tk.IntVar()


def data_type_button(name_tag, variable):
    button = tk.Checkbutton(window, text=name_tag, variable=variable, command=lambda: 1)
    button.pack()
    button.select()


data_type_button("Confirmed Cases", var_confirmed)
data_type_button("Deaths", var_deaths)
data_type_button("Recoveries", var_recoveries)
data_type_button("Daily", var_daily)

ok = tk.Button(window, text="OK", width=20, command=SUBMIT)
ok.pack()

tk.mainloop()