from tkinter import *
import tkinter.font
from socket import AF_INET,socket,SOCK_STREAM
from threading import Thread
from PIL import ImageTk,Image
import os
import pickle



class WarningButton(Button):
    def __init__(self,*args,**kwargs):
        Button.__init__(self,*args,**kwargs)
    def clickAlreadyExistWarningSignButton(self,event):
        already_exist_canves.place_forget()
        fullname.configure(state=NORMAL)
        username_signup.configure(state=NORMAL)
        password_signup.configure(state=NORMAL)
        signup_button.configure(state=NORMAL)
    def clickSignInErrorButton(self,event):
        signin_error_canves.place_forget()
        username.configure(state=NORMAL)
        password.configure(state=NORMAL)
        signin_button.configure(state=NORMAL)
    def clickEmptySignUpInputMethod(self,event):
        empty_input_canves.place_forget()
        fullname.configure(state=NORMAL)
        username_signup.configure(state=NORMAL)
        password_signup.configure(state=NORMAL)
        signup_button.configure(state=NORMAL)



class MainCanvasButtons(Button):
    def __init__(self,*args,master,bg,**kwargs):
        Button.__init__(self, *args,master=master,bg=bg, **kwargs)
        self.defaultBackground = self["background"]
        self.bind('<Button-1>',self.on_click)
        self.bind('<Enter>',self.on_enter)
        self.bind('<Leave>',self.on_leave)
    def on_click (self,event):
        self.configure(bd=0,bg="#6e8387",highlightthickness=0, relief='ridge')
    def on_enter (self,event):
        self['background'] = self['activebackground']
    def on_leave (self,event):
        self['background'] = self.defaultBackground
    def revealLeftArrowAndSearchBar(self, event):
        left_arrow_button.place(relx = 0.008,rely=0.25)
        searchBox.place(width=450,height=30,relx = 0.12,rely=0.026)
    def removeLeftArrowAndSarchBar(self, event):
        left_arrow_button.place_forget()
        searchBox.place_forget()
        search_global.place_forget()
        searchBox.delete(0, END)
        search_result_username.delete('1.0', END)
        search_result_fullname.delete('1.0', END)


class ChatCanvasButtons(Button):
    def __init__(self, *args, master, bg, **kwargs):
        Button.__init__(self, *args, master=master, bg=bg, **kwargs)
        self.defaultBackground = self["background"]
        self.bind('<Button-1>', self.on_click)
        self.bind('<Enter>', self.on_enter)
        self.bind('<Leave>', self.on_leave)

    def on_click(self, event):
        self.configure(bd=0, bg="#6e8387", highlightthickness=0, relief='ridge')

    def on_enter(self, event):
        self['background'] = self['activebackground']

    def on_leave(self, event):
        self['background'] = self.defaultBackground

    def on_click(self, event):
        self.configure(bd=0, highlightthickness=0, relief='ridge')
    def returnToMainWindow(self,event):
        chat_window.pack_forget()
        top.geometry("600x800")
        main_window.propagate(0)
        main_window.pack()
        bar_label.place(relx=0, rely=0)
        search_button.place(relx=0.88, rely=0.25)
        search_button.bind('<Button-1>', search_button.revealLeftArrowAndSearchBar)
        searchBox.bind('<KeyRelease>', sendSearchInput)
        left_arrow_button.bind('<Button-1>', left_arrow_button.removeLeftArrowAndSarchBar)






HEADER_SignUp = 'SIGNUP'
HEADER_already_exist = 'AlreadyExist'
HEADER_SignIn = 'SIGNIN'
HEADER_Start = 'START'
HEADER_msg ='MSG'
HEADER_REC = 'REC'
HEADER_Search = 'SEARCH'
HEADER_NonEx = 'SEARCH_NonEXIST'
previously_contacted = []
###Backend###
PORT =5051
BUFSIZ = 1024
cleint_socket = socket(AF_INET, SOCK_STREAM)
cleint_socket.connect(('127.0.1.1', PORT))

'''def on_closing(event =None):
    cleint_socket.close()'''


class Node:
    def __init__(self,header,sent_msg,received_msg,next_node=None):
        self.sent_msg = sent_msg
        self.received_msg = received_msg
        self.header = header
        self.next_node = next_node
        self.list_sent =[]
        self.list_sent.append(self.sent_msg)
        self.list_received = []
        self.list_received.append(self.received_msg)
        self.grid_cord_available = (0.1,0.5)
    def get_list_sent(self):
        return self.list_sent

    def get_grid_cord_available(self):
        return self.grid_cord_available

    def reset_grid_cord_available(self, x, y):
        self.grid_cord_available  = (x, y)

    def get_list_received(self):
        return self.list_received

    def get_header(self):
        return self.header

    def append_to_list_sent(self, sent_msg):
        self.list_sent.insert(0,sent_msg)

    def append_to_list_received(self,received_msg):
        self.list_received.insert(0,received_msg)

    def get_next_node(self):
        return self.next_node

    def set_next_node(self, next_node):
        self.next_node = next_node


class MessageSession:
    def __init__(self, sent_msg = "nothing",received_msg =None,header = None):
        self.head_node = Node(sent_msg,received_msg,header)

    def get_head_node(self):
        return self.head_node

    def insert_beginning(self,new_sent_msg = "nothing-first" , header = None,new_received_msg = None):
        new_node = Node(sent_msg = new_sent_msg, received_msg = new_received_msg, header = header)
        new_node.set_next_node(self.head_node)
        self.head_node = new_node


    def add_new_msg(self, header , sent_msg= "nothingnew" , received_msg = None ):
        current_node = self.get_head_node()
        for _ in previously_contacted:
            if current_node.get_header() == header:
                 current_node.append_to_list_received(received_msg)
                 current_node.append_to_list_sent(sent_msg)
            else:
                current_node = current_node.get_next_node()

    def show_latest_msg (self):
        current_node = self.get_head_node()
        current_node.reset_grid_cord_available(x=0.1, y=0.5 )
        for _ in range(len(previously_contacted)):
            if current_node.get_header() == search_result_username.get('1.0', END):
                for i in current_node.get_list_sent():
                    X, Y = current_node.get_grid_cord_available()
                    msg_label = Label(chat_window ,text = i)
                    msg_label.propagate = 1
                    msg_label.place(relx = X ,rely = Y)
                    current_node.reset_grid_cord_available(x=X , y=Y - 0.03)


    def stringify_list_sent(self,header):
        current_node = self.get_head_node()
        for i in range(len(previously_contacted)):
            print(current_node.get_list_sent())
            print("sent")
            #current_node = current_node.get_next_node()


    def stringify_list_received(self,header):
        current_node = self.get_head_node()
        for i in range(len(previously_contacted)):
            print(current_node.get_list_received())
            print("reveived")
            #current_node = current_node.get_next_node()


def send_usernameANDpassword_atSignUp(event = None):
    full_name = fullname_stringvar.get()
    full_name = HEADER_SignUp+full_name
    usrname = username_signup_stringvar.get().strip()
    usrname= HEADER_SignUp+usrname
    passwd = password_signup_stringvar.get()
    passwd = HEADER_SignUp+passwd
    if str(full_name) == HEADER_SignUp or fullname.figureIfEmpty ==False :
        fullname.configure(bg = 'red')
        if str(usrname) == HEADER_SignUp or username_signup.figureIfEmpty ==False:
            username_signup.configure(bg = 'red')
            if str(passwd) == HEADER_SignUp or password_signup.figureIfEmpty ==False:
                password_signup.configure(bg = 'red')
    else:
        cleint_socket.send(bytes(usrname+'||'+passwd+'||'+full_name, 'utf8'))
def send_usernameANDpassword_atSignIn(event = None):
    username_signin = username_stringvar.get()
    username_signin = HEADER_SignIn+username_signin
    passwd_signin = password_stringvar.get()
    passwd_signin = HEADER_SignIn+passwd_signin
    if str(username_signin) == HEADER_SignIn or username.figureIfEmpty == False:
        username.configure(bg='red')
        if str(passwd_signin) == HEADER_SignIn or password.figureIfEmpty == False:
            password.configure(bg='red')
    else:
        cleint_socket.send(bytes(username_signin + '||' + passwd_signin , 'utf8'))

def sendSearchInput(event):
    search_input = HEADER_Search+searchBox_stringvar.get()
    if str(search_input) == HEADER_Search:
        search_global.place_forget()
    else :
       cleint_socket.send(bytes(search_input, 'utf8'))


new_node = MessageSession()
def sendMassage(event):
    if chat_entry.get() != '' :
        msg_sent = HEADER_msg+search_result_username.get('1.0', END)+chat_entry.get()
        user_receiving = search_result_username.get('1.0', END)
        if not user_receiving in previously_contacted :
            cleint_socket.send(bytes(msg_sent , 'utf8'))
            new_node.insert_beginning(new_sent_msg=chat_entry.get(),header=user_receiving)
            previously_contacted.insert(0,user_receiving)
            new_node.stringify_list_sent(header=user_receiving)
            #new_node.show_latest_msg()
        else:
            cleint_socket.send(bytes(msg_sent , 'utf8'))
            previously_contacted.remove(user_receiving)
            previously_contacted.insert(0,user_receiving)
            new_node.add_new_msg(header= user_receiving,sent_msg=chat_entry.get())
            new_node.stringify_list_sent(header=user_receiving)
            #new_node.show_latest_msg()
        chat_entry.delete(0, END)


def receiveMassage(sender, new_msg):
    if not sender in previously_contacted:
        new_node.insert_beginning(header = sender ,new_received_msg  =new_msg)
        previously_contacted.insert(0, sender)
        new_node.stringify_list_received(header=sender)
    else:
        previously_contacted.remove(sender)
        previously_contacted.insert(0, sender)
        new_node.add_new_msg(header=sender, received_msg=new_msg)
        #new_node.stringify_list_received(header=sender)


#***Cleint Front end***#
top = Tk()
top.title("Zajel")
top.geometry("600x600")
top.configure(bg='#C8D3D5' )
FontOfEntryList= tkinter.font.Font(family="Calibri",size=18)
FontOfSearchResult_Fullname = tkinter.font.Font(family="Calibri",size=22)
FontOfSearchResult_Username = tkinter.font.Font(family="Calibri",size=12)
FontOfSearchBox= tkinter.font.Font(family="Calibri",size=14)
FontOfChatBar = tkinter.font.Font(family="Calibri",size=25)

#top.protocol("WM_DELETE_WINDOW", on_closing)

#FontOFLabelsText = tkinter.font.Font()
#The sign in Canves
###Here is the Entry Box for the sign in and sign up pages###

class Ebox(Entry):
    def __init__(self,default_text='',master=None, *args, **kwags):
        Entry.__init__(self,master, *args, **kwags)
        self.insert(0, default_text)
        self.figureIfEmpty = False
        #self.configure(state=DISABLED)
        self.on_click_id = self.bind('<Button-1>', self.on_click)
    def on_click(self, _):
        #self.configure(state=NORMAL)
        self.figureIfEmpty =True
        self.delete(0, END)
        # unregister myself
        self.unbind('<Button-1>', self.on_click_id)
        self.configure(bg = 'white')



main_window = Canvas (top, width=600,bg='#C8D3D5',height=800,bd=0, highlightthickness=0, relief='ridge')
#Images
Image_Path ="/home/almaliki565/PycharmProjects/Chatapp/venv/Icones folder/"
search_image = Image.open(os.path.join(Image_Path,"Search.png"))
search_photo = ImageTk.PhotoImage(search_image)
left_arrow_image = Image.open(os.path.join(Image_Path,"LeftArrow.png"))
left_arrow_photo= ImageTk.PhotoImage(left_arrow_image)
bar_image = Image.open(os.path.join(Image_Path,"bar.png"))
bar_photo =  ImageTk.PhotoImage(bar_image)
###
bar_label = Label( main_window,image=bar_photo,bd = 0,bg ="#6E8387", highlightthickness=0, relief='flat')
search_button = MainCanvasButtons(master=bar_label,image=search_photo,bg="#6E8387",activebackground="#6E8387",  highlightthickness=0, relief='flat')
left_arrow_button = MainCanvasButtons(master=bar_label,bg="#6E8387",  highlightthickness=0, relief='flat',activebackground="#6E8387",image=left_arrow_photo)
searchBox_stringvar = StringVar()
searchBox  =  Entry(main_window,bg='white',font=FontOfSearchBox, relief=FLAT,
                       borderwidth=15,textvariable=searchBox_stringvar)

search_global = Label(main_window)

search_result_fullname = Text(search_global, height=1, width=50, font=FontOfSearchResult_Fullname, highlightthickness=0,
                              relief='ridge', bd=0)
search_result_username = Text(search_global, height=1, width=50, font=FontOfSearchResult_Username,
                              highlightthickness=0, relief='ridge', bd=0)

def creatMainWindow():
    list_of_logging_canvases = [sign_up ,sign_in]
    for i in list_of_logging_canvases:
        i.place_forget()
    top.geometry("600x800")
    main_window.propagate(0)
    main_window.pack()
    bar_label.place(relx = 0 , rely = 0)
    search_button.place(relx = 0.88,rely=0.25)
    search_button.bind('<Button-1>',search_button.revealLeftArrowAndSearchBar)
    searchBox.bind('<KeyRelease>',sendSearchInput)
    left_arrow_button.bind('<Button-1>',left_arrow_button.removeLeftArrowAndSarchBar)

###Chat Canvas###
chat_window = Canvas (top, width=600,bg='#C8D3D5',height=800,bd=0, highlightthickness=0, relief='ridge')
chat_entry_image = Image.open(os.path.join(Image_Path,"chat_bar.png"))
chat_entry_photo = ImageTk.PhotoImage(chat_entry_image)
send_image = Image.open(os.path.join(Image_Path,"send.png"))
send_photo = ImageTk.PhotoImage(send_image)
send_button = ChatCanvasButtons(master=chat_window,image=send_photo,bg="#C8D3D5",activebackground="#C8D3D5",  highlightthickness=0, relief='flat')
chat_entry_label = Label(chat_window,image=chat_entry_photo,bd = 0,bg ="#6E8387", highlightthickness=0, relief='flat')
bar_label_chat =  Label( chat_window,image=bar_photo,bd = 0,bg ="#6E8387", highlightthickness=0, relief='flat')
left_arrow_button_chat = ChatCanvasButtons(master=bar_label_chat,bg="#6E8387",  highlightthickness=0, relief='flat',activebackground="#6E8387",image=left_arrow_photo)
fullname_chat = Text(bar_label_chat, height=1, width=50,bg ="#6E8387",font = FontOfChatBar, highlightthickness=0,
                              relief='ridge', bd=0)
chat_entry = Entry(chat_entry_label,font =FontOfEntryList,relief=FLAT,highlightthickness=0)
def creatChatWindow(event):
    search_result_username.delete('1.0', END)
    search_result_fullname.delete('1.0', END)
    search_global.place_forget()
    main_window.pack_forget()
    searchBox.delete(0, END)
    search_result_username.delete('1.0', END)
    search_result_fullname.delete('1.0', END)
    search_global.place_forget()
    top.geometry("600x800")
    chat_window.propagate(0)
    chat_window.pack()
    bar_label_chat.propagate(0)
    bar_label_chat.place(relx=0,rely=0)
    left_arrow_button_chat.place(relx=0.008, rely=0.25)
    fullname_chat.propagate(0)
    fullname_chat.place(relx = 0.13,rely = 0.25)
    chat_entry_label.propagate(0)
    chat_entry_label.place(relx=0.01,rely=0.92)
    chat_entry.pack(ipadx = 140, ipady = 40,pady =5)
    send_button.place(relx = 0.9,rely = 0.92)
    chat_window.bind("<1>", lambda event: chat_window.focus_set())
    chat_window.bind("<Left>", lambda event: chat_window.xview_scroll(-1, "units"))
    chat_window.bind("<Right>", lambda event: chat_window.xview_scroll(1, "units"))
    chat_window.bind("<Up>", lambda event: chat_window.yview_scroll(-1, "units"))
    chat_window.bind("<Down>", lambda event: chat_window.yview_scroll(1, "units"))
    chat_window.focus_set()
    left_arrow_button_chat.bind('<Button-1>',left_arrow_button_chat.returnToMainWindow)
    chat_entry.bind('<Return>',sendMassage)
    send_button.bind('<Button-1>',sendMassage)


class Ebox(Entry):
    def __init__(self,default_text='',master=None, *args, **kwags):
        Entry.__init__(self,master, *args, **kwags)
        self.insert(0, default_text)
        self.figureIfEmpty = False
        #self.configure(state=DISABLED)
        self.on_click_id = self.bind('<Button-1>', self.on_click)
    def on_click(self, _):
        #self.configure(state=NORMAL)
        self.figureIfEmpty =True
        self.delete(0, END)
        # unregister myself
        self.unbind('<Button-1>', self.on_click_id)
        self.configure(bg = 'white', highlightthickness=0)






def toSignin (event):
    sign_up.place_forget()
    sign_in.place(relx=0.075, rely=0.3)
    username.pack(ipadx=120, ipady=10,pady =10)
    password.pack(ipadx=120, ipady=10)
    signin_button.place(relx=0.43, rely=0.4)
    tosignup_button.place(relx=0.33, rely=0.6)
    tosignup_button.bind("<Button-1>", tosignup_button.show_signup_page)


sign_up = Canvas(top, width=500,bg='#C8D3D5',height=700,bd=0, highlightthickness=0, relief='ridge')
sign_up.propagate(0)
fullname_stringvar = StringVar()
fullname = Ebox(master=sign_up, font=FontOfEntryList, default_text="Full Name.", relief=FLAT,
                       borderwidth=15, textvariable=fullname_stringvar)
username_signup_stringvar = StringVar()
username_signup = Ebox(master=sign_up, font=FontOfEntryList, default_text="Username or Phone.", relief=FLAT,
                       borderwidth=15, textvariable=username_signup_stringvar)
password_signup_stringvar = StringVar()
password_signup = Ebox(master=sign_up, font=FontOfEntryList, default_text="Password.", relief=FLAT,
                       borderwidth=15,textvariable=password_signup_stringvar)
signup_button = Button(master=sign_up, bg='#cbdff2', text="Sign Up", relief=FLAT,command = send_usernameANDpassword_atSignUp)
tosignin_button = Button(master=sign_up, bd=0,fg='white', highlightthickness=0, relief='ridge',font = FontOfEntryList,bg='#C8D3D5', text="Sign In")
tosignin_button.bind('<Button-1>', toSignin)

already_exist_canves = Canvas(sign_up, width=120, bg='white', height=30, bd=0, highlightthickness=0,
                                    relief='ridge')
already_exist_label = Label(master=already_exist_canves, text="Username Already Exist.", bg='white', height=5, width=28,
                            font=FontOfEntryList)



def receiving_warnings():
    while True:
        warning_msg = cleint_socket.recv(BUFSIZ).decode("utf8")
        if warning_msg.startswith(HEADER_msg):
            warning_msg = warning_msg.replace(HEADER_msg, '')
            msg_obtained = warning_msg.split(HEADER_REC)
            #if  msg_obtained[0] != username_signup_stringvar.get().strip()  and msg_obtained[0] != username_stringvar.get():
            receiveMassage(sender=msg_obtained[0],new_msg=msg_obtained[1])

        if warning_msg.startswith(HEADER_already_exist):
            fullname.configure(state=DISABLED)
            username_signup.configure(state=DISABLED)
            password_signup.configure(state=DISABLED)
            signup_button.configure(state=DISABLED)

            already_exist_canves.propagate(1)
            already_exist_canves.place(relx=0.15, rely=0.2)
            already_exist_label.propagate(0)
            already_exist_label.pack()
            already_exist_button =WarningButton(master=already_exist_canves,bd=0,fg='black' ,bg ='white',highlightthickness=0, relief='ridge', text="OK")
            already_exist_button.place(relx=0.4, rely=0.6)
            already_exist_button.bind("<Button-1>",already_exist_button.clickAlreadyExistWarningSignButton)


        elif  warning_msg.startswith(HEADER_SignIn):
            username.configure(state=DISABLED)
            password.configure(state=DISABLED)
            signin_button.configure(state=DISABLED)

            signin_error_canves.propagate(1)
            signin_error_canves.place(relx=0.15, rely=0.2)

            signin_error_label.propagate(0)
            signin_error_label.pack()
            signin_error_button = WarningButton(master=signin_error_canves, bd=0, fg='black', bg='white',
                                                  highlightthickness=0, relief='ridge', text="OK")
            signin_error_button.place(relx=0.4, rely=0.6)
            signin_error_button.bind("<Button-1>", signin_error_button.clickSignInErrorButton)
        elif  warning_msg.startswith(HEADER_Start):
             creatMainWindow()

        elif  warning_msg.startswith(HEADER_NonEx) :
            search_global.place_forget()
        elif  warning_msg.startswith(HEADER_Search) :
            warning_msg = warning_msg.replace(HEADER_Search, '').split('||')
            search_result_fullname.configure(state=NORMAL)
            search_result_username.configure(state=NORMAL)
            search_result_username.delete('1.0', END)
            search_result_fullname.delete('1.0', END)
            search_result_fullname.insert(END,' '+warning_msg[0]+'\n')
            search_result_username.insert(END,warning_msg[1])
            search_result_fullname.configure(state=DISABLED)
            search_result_username.configure(state=DISABLED)
            #bar_label_chat.configure(text = warning_msg[0])
            fullname_chat.configure(state=NORMAL)
            fullname_chat.delete('0.1', END)
            fullname_chat.insert(END,search_result_fullname.get('1.0',END))
            fullname_chat.configure(state=DISABLED)
            search_global.propagate(0)
            search_global.configure(height =4, width =60,bg ='white',highlightthickness=0,
                                    relief='ridge' , bd = 0)
            search_global.bind("<Button-1>",creatChatWindow)
            search_global.place(relx = 0.091,rely=0.065)
            #search_result_fullname.configure(state=DISABLED)
            #search_result_username.configure(state=DISABLED)
            search_result_fullname.bind("<Button-1>",creatChatWindow)
            search_result_username.bind("<Button-1>",creatChatWindow)
            search_result_fullname.pack()
            search_result_username.pack()
            warning_msg.clear()


class ShowSignUp(Button):
    def __init__(self,  master=None, *args, **kwags):
        Button.__init__(self, master, *args, **kwags)
    def show_signup_page (self,event):

        sign_in.place_forget()

        sign_up.place(relx=0.075, rely=0.3)
        fullname.pack(ipadx=120, ipady=10)
        username_signup.pack(ipadx=120, ipady=10,pady = 10)
        password_signup.pack(ipadx=120, ipady=10)
        signup_button.place(relx=0.42, rely=0.4)
        tosignin_button.place(relx=0.4, rely=0.5)



sign_in = Canvas(top, bg='#C8D3D5', width=500, height=500, bd=0, highlightthickness=0, relief='ridge')
sign_in.propagate(0)
username_stringvar = StringVar()
username = Ebox(master=sign_in, font=FontOfEntryList, default_text="Username or Phone.", relief=FLAT,
                borderwidth=15,textvariable=username_stringvar)
password_stringvar = StringVar()
password = Ebox(master=sign_in, font=FontOfEntryList, default_text="Password.", relief=FLAT, borderwidth=15,textvariable=password_stringvar)
signin_button = Button(master=sign_in, bg='#cbdff2', text="Log in", relief=FLAT, command=send_usernameANDpassword_atSignIn)

tosignup_button = ShowSignUp(master=sign_in,fg='white',bd=0, highlightthickness=0, relief='ridge',font = FontOfEntryList,bg='#C8D3D5', text="Sign Up at Zajel")
tosignup_button.place(relx=0.33, rely=0.6)
tosignup_button.bind("<Button-1>", tosignup_button.show_signup_page)
signin_error_canves = Canvas(sign_in, width=120, bg='white', height=30, bd=0, highlightthickness=0,
                                    relief='ridge')

signin_error_label = Label(master=signin_error_canves, text="Wrong Username or Password.", bg='white', height=5,
                                        width=28, font=FontOfEntryList)


class ShowSignIn (Canvas):
    def __init__(self, default_text='', master=None, *args, **kwags):
       Canvas.__init__(self, master, *args, **kwags)

    sign_up.place_forget()
    sign_in.place(relx=0.075, rely=0.3)
    username.pack(ipadx=120, ipady=10, pady = 10)
    password.pack(ipadx=120, ipady=10)
    signin_button.place(relx = 0.43,rely=0.4)

    tosignup_button.place(relx = 0.33,rely=0.6)
    tosignup_button.bind("<Button-1>",tosignup_button.show_signup_page)


sigin = ShowSignIn()

receiving_warnings_thread = Thread(target=receiving_warnings)
receiving_warnings_thread.start()

top.mainloop()
