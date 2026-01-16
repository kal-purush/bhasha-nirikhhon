import tkinter as tk
from openai import OpenAI
import sqlite3_connector
import sqlite3_searcher
#初始化tkinter
win = tk.Tk()
win.title('text')
win.geometry('800x800')

def gpt():
    global send_message,ans
    client = OpenAI(
        api_key='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'
    )
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": send_message}
        ]
    )
    ans = completion.choices[0].message
def question():
    global ans,send_message
    cannot_send_messages = ['']
    send_message = input()  #输入问题
    if sqlite3_searcher.search(send_message) != '':
        print(sqlite3_searcher.search(send_message))
    else:
        for i in cannot_send_messages:
            if i in send_message:
                print('包含敏感信息,无法发送')
                return 1
        gpt()
        sqlite3_connector.sql_add(ans,send_message)


#tkinter
First_Label = tk.Label(win,text='Text')
First_Label.pack()
Question_Entry = tk.Entry(win,width=40)
Question_Entry.pack()
Search_question_Button = tk.Button(win,text='提问',command=question)
Search_question_Button.pack()
ANS_Label = tk.Label(win,text='回答:')
ANS_Label.pack()
win.mainloop()