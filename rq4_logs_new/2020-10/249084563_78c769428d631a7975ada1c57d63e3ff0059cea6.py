# -*- coding: utf-8 -*-
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


def SUBMIT():
    values = name.get()
    all_data = dict()
    try:
        if var_confirmed.get() == 1:
            all_data['Confirmed'] = retrieve_data("Confirmed", values)
        if var_deaths.get() == 1:
            all_data['Deaths'] = retrieve_data("Deaths", values)
        if var_recoveries.get() == 1:
            all_data['Recoveries'] = retrieve_data("Recoveries", values)
    except KeyError:
        tk.messagebox.showerror(title='Error', message="There's not country like: '"+values+"'!")
        return

    if len(all_data) == 0:
        tk.messagebox.showerror(title='Error', message='No selected data!')
        return

    df = pd.DataFrame(all_data, index=retrieve_index(values))
    df.plot(figsize=(12, 8), title='COVID-19 in ' + values)

    plt.savefig("images/chart.png")
    img2 = ImageTk.PhotoImage(Image.open("images/chart.png"))
    panel.configure(image=img2)
    panel.image = img2


window = tk.Tk()
window.title("Check the COVID-19")
window.wm_resizable(800, 750)

img = Image.open("images/initial.png")
img = img.resize((250, 250), Image.ANTIALIAS)
img = ImageTk.PhotoImage(img)
panel = tk.Label(window, image=img)
panel.pack(side="bottom", fill="both", expand="yes")

text = tk.StringVar()
label = tk.Label(window, textvariable=text)
label.pack()

description = tk.Label(window, text="Please enter your country/province/state/region").pack()
name = tk.Entry(window, width=40)
name.pack()

var_confirmed = tk.IntVar()
var_deaths = tk.IntVar()
var_recoveries = tk.IntVar()


def data_type_button(name_tag, variable):
    button = tk.Checkbutton(window, text=name_tag, variable=variable, command=lambda: 1)
    button.pack()
    button.select()


data_type_button("Confirmed Cases", var_confirmed)
data_type_button("Deaths", var_deaths)
data_type_button("Recoveries", var_recoveries)

ok = tk.Button(window, text="OK", width=20, command=SUBMIT)
ok.pack()

tk.mainloop()