from covid import Covid
from tkinter import *
#BLL
def common():
    code= msg.get()
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases

def func1():
    code = "India"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"]=cases
def func2():
    code = "france"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func3():
    code = "spain"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func4():
    code = "italy"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func5():
    code = "brazil"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func6():
    code = "china"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func7():
    code = "United Kingdom"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func8():
    code = "Germany"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func9():
    code = "us"
    covid = Covid()
    cases = covid.get_status_by_country_name(code)
    lb["text"] = cases
def func10():
    root.destroy()

#PL
root=Tk()
root.title("COVID-19 LIVE UPDATE")
root.geometry("2000x1000")
root.config(bg='lightblue')
lbl1=Label(root,text="COVID-19 UPDATE (Stay Updated,Stay Alert)",font='verdana 17 bold',bg="red")
lbl1.place(x=400,y=10)
lbl2=Label(root,text="Enter Any Country Name:",font=2,bg="yellow")
lbl2.place(x=10,y=80)
lbl1=Label(root,text="RESULT:",font=2,bg="yellow")
lbl1.place(y=450)
lb = Label(root, text="_______________________________________________________", font=20, bg="blue", fg="yellow")
lb.place(y=490)
msg=StringVar()
enterytext = Entry(root,textvariable=msg,font=23)
enterytext.place(x=260,y=80)
btn1=Button(root,text="INDIA",font=20,fg="white",bg="orange",command=func1)
btn1.place(x=50,y=200)  
btn2=Button(root,text="FRANCE",font=20,fg="white",bg="orange",command=func2)
btn2.place(x=190,y=200)
btn3=Button(root,text="SPAIN",font=20,fg="white",bg="orange",command=func3)
btn3.place(x=350,y=200)
btn4=Button(root,text="ITALY",font=20,fg="white",bg="orange",command=func4)
btn4.place(x=50,y=270)
btn5=Button(root,text="BRAZIL",font=20,fg="white",bg="orange",command=func5)
btn5.place(x=190,y=270)
btn6=Button(root,text="CHINA",font=20,fg="white",bg="orange",command=func6)
btn6.place(x=350,y=270)
btn7=Button(root,text="UK",font=20,fg="white",bg="orange",command=func7)
btn7.place(x=66,y=330)
btn8=Button(root,text="GERMANY",font=20,fg="white",bg="orange",command=func8)
btn8.place(x=190,y=330)
btn9=Button(root,text="US",font=20,fg="white",bg="orange",command=func9)
btn9.place(x=360,y=330)
btn10=Button(root,text="              EXIT                 ",font=20,fg="white",bg="orange",command=func10)
btn10.place(x=100,y=390)
btn11=Button(root,text="SEARCH",font=20,fg="white",bg="orange",command=common)
btn11.place(x=490,y=70)
mainloop()