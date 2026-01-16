import socket,os,subprocess,glob,urllib,re,smtplib
import tkFileDialog
from bs4 import BeautifulSoup as bs
from PIL import ImageTk,Image
from threading import Thread
from SocketServer import ThreadingMixIn
from subprocess import Popen, PIPE
import tkMessageBox
try:
    # for Python2
    import Tkinter as tk
    import Tkinter.ttk as ttk
    print"try"
except ImportError:
    # for Python3
    print "except"
    import tkinter as tk
    import tkinter.ttk as ttk

root = tk.Tk()

connected_str=tk.StringVar()
connected=[]

def openfile(*args):
	print text_1.get()

class ClientThread(Thread):

    def __init__(self,addr,conn,filename):
        Thread.__init__(self)
        self.addr = addr
        self.conn = conn
        self.filename = filename
        print " New thread started for "+str(addr)+str(filename)
        
    def run(self):
    	filename=self.filename
    	conn=self.conn
        f = open(filename,'rb')
        while True:
			if filename:
				#conn.send(str(int(os.path.getsize(filename))))
				f1=open(filename,'rb')
				print "sending "+filename+"..."
				bytesToSend = f1.read(1024000)
				while bytesToSend:
					#print "sending"
					conn.send(bytesToSend)
					bytesToSend = f1.read(1024000)
				f1.close()
			#c.close()	

class ServerThread(Thread):

    def __init__(self):
        Thread.__init__(self)
       	print "trying to start server"

    def run(self):
		try:
			host = '0.0.0.0'
			port=8000	
			sock=socket.socket()
			sock.bind((host,port))
			button_2.config(state="disabled")
			button_1.config(state="disabled")
		except:
			tkMessageBox.showerror("Error","Server already started ..\n Try acting as client")
			print "server already started ..."
		threads=[]
		#while True:
		sock.listen(5)
		for i in range(5):
			print "Waiting for incoming connections..."
			(conn, addr) = sock.accept()
			print 'Got connection from ',(addr)
			#filename=conn.recv(1024)
			formats=["mp4","mkv", "3gp","asf", "AVI", "DVR-MS", "FLV","mp3","wma","MIDI","mov","Ogg", "OGM", "WAV", "MPEG-2", "AIFF"]
			files=[]
			for i in formats:
				files=files+(glob.glob("*"+i))
			f_list=""
			for i in files:
				f_list=f_list+"-->  "+i+"\n"
			c_name=conn.recv(1024)
			print c_name
			connected.append(c_name)
			connected_str.set("server connected to "+str(connected))
			conn.send(f_list)
			print "sending "+f_list+"..."
			filename=conn.recv(1024)
			print "chosen file is "+str(filename)
			newthread = ClientThread(addr,conn,filename)
			newthread.start()
			threads.append(newthread)
		
def server(*args):
	threads_server=[]
	serverthread=ServerThread()
	serverthread.start()
	threads_server.append(serverthread)
	#for t in threads:
	#	t.join()

def client(*args):
	cmdline = ['C:/Program Files (x86)/VideoLAN/VLC/vlc.exe', '-']
	host="192.168.1.100"
	port=8000
	try:
		s=socket.socket()
		s.connect((host,port))
		button_1.config(state="disabled")
	except:
		tkMessageBox.showerror("Error","Server not started ..\n")
		print "server not started .."
	if socket.gethostname().find('.')>=0:
		name=socket.gethostname()
	else:
		name=socket.gethostbyaddr(socket.gethostname())[0]
	s.send(name)
	filenames=s.recv(1024)
	print filenames
	popup = tk.Toplevel()
	print "hahahah"
	p_frame=tk.Frame(popup)
	p_frame.pack()
	label_2=tk.Label(p_frame,text="choose one of the files to play -:")
	label_2.grid(row=1,column=1)
	status = tk.StringVar()
	status_label=tk.Label(p_frame,textxtvariable=status,wraplength=105)
	status_label.grid(row=2,column=1)
	status.set(filenames)
	file_name=tk.StringVar()
	entry_1=tk.Entry(p_frame,textvariable=file_name)
	entry_1.grid(row=3,column=1,pady=10)

	def send(*args):
		s.send(str(file_name.get()))
		f=open("new_"+file_name.get(),'wb')
		i=0.000
		vlc=subprocess.Popen(cmdline,stdin=subprocess.PIPE)
		popup.destroy()
		while(True):
			data=s.recv(1024000)
			while(data):
				i=i+1
				f.write(data)
				vlc.stdin.write(data)
				data=s.recv(1024000)                             
			f.close()
	button_3=tk.Button(p_frame,command=send,text="Play")
	button_3.grid(row=3,column=2,padx=2)
	#filename = raw_input("Enter the filename :")
	
			
	#f.close()

mainframe = tk.Frame(root,borderwidth=1,relief='solid')
mainframe.grid(column=0, row=0)


n = ttk.Notebook(mainframe,height=700,width=1350,style="BW.TNotebook")
n.grid(column=1,row=1)
f1 = tk.Frame(n)
#f2 = tk.Frame(n)
f3 = tk.Frame(n)
f4 = tk.Frame(n)
n.add(f1, text='    MASS STREAMER   ')
#n.add(f2, text='    FILE Transfer   ')
n.add(f3, text='    MASS EMAIL   ')
n.add(f4, text='    GET IT ALL   ')

#contents of streamer

background_image=ImageTk.PhotoImage(Image.open("back.jpg"))
background_label = tk.Label(f1, image=background_image)
background_label.place(x=0, y=0, relwidth=1, relheight=1)

void=tk.Frame(f1).grid(column=2,row=4,pady=80,padx=45)
button_1=tk.Button(f1,text="Start Server",font="Calibri 15",width=30,command=server,height=2)
button_1.grid(column=4,row=10,pady=70,padx=370)
button_2=tk.Button(f1,text="Act as Client",font="Calibri 15",command=client,width=30,height=2)
button_2.grid(column=4,row=11)
label_8=tk.Label(f1,font="Calibri 15",textvariable=connected_str)
label_8.grid(column=4,row=18,pady=200)
connected_str.set(str(connected))
#contents of GET IT ALL

def get_it_all(*args):
	option=tk.StringVar()
	def download(*args):
			o_sel=int(option.get())-1
			if not os.path.exists(url2.split("/")[2]):
				os.makedirs(url2.split("/")[2])
			try:
					n=0
					src=urllib.urlopen(a[o_sel])
					name=a[o_sel].split("/")[-1]
					src_bin=src.read()
					f=open(os.getcwd()+"\\"+url2.split("/")[2]+"\\"+name, 'wb')
					f.write(src_bin)
					f.close()
					n=n+1
			except:
					name=a[o_sel].split("/")[-1]
					print name
					print "not done"
	def download_all():
		if not os.path.exists(url2.split("/")[2]):
			os.makedirs(url2.split("/")[2])
		for i in a:
			try:
				n=0
				src=urllib.urlopen(i)
				name=i.split("/")[-1]
				src_bin=src.read()
				f=open(os.getcwd()+"\\"+url2.split("/")[2]+"\\"+name, 'wb')
				f.write(src_bin)
				f.close()
				n=n+1
			except:
				name=i.split("/")[-1]
				print name
				print "not done"
	get_window=tk.Toplevel(master=root)
	url=url_entered.get()
	res=urllib.urlopen(url)
	done=0
	try:
		url2=re.search("(.*).com",url).group()
		done=1
	except:
		pass
	try:
		if done==0:
			url2=re.search("(.*).in",url).group()
	except:
		pass
	if url2.endswith("/"):
		url2=url2[:-1]
	html=res.read()
	a=re.findall('src="([^\s]+)"',html,re.M|re.I)
	for i in range(len(a)):
		if str(a[i]).startswith("/")==True:
			a[i]=url2+str(a[i])
		if str(a[i]).startswith("http://")==False and str(a[i]).startswith("https://")==False and str(a[i]).startswith("/")==False:
			a[i]=url+str(a[i])
	print a
	f_list=""
	p=1
	for i in a:
		f_list=f_list+str(p)+".) "+i.split("/")[-1]+"\n"
		p=p+1
	label_4=tk.Label(get_window,text="Select an option to download :")
	label_4.grid(row=0,column=0,padx=10)
	d_files=tk.StringVar()
	label_3=tk.Label(get_window,textvariable=d_files)
	label_3.grid(row=1,column=1,pady=10)
	d_files.set(f_list)
	entry_3=tk.Entry(get_window,width=40,textvariable=option,background="#ffffff")
	entry_3.grid(row=2,column=1,padx=10)
	button_5=tk.Button(get_window,text="download",command=download)
	button_5.grid(row=3,column=1)
	button_6=tk.Button(get_window,text="download_all",command=download_all)
	button_6.grid(row=3,column=2,pady=5,padx=10)
	
	print "everything done"

background_image_4=ImageTk.PhotoImage(Image.open("getitall.jpg"))
background_label_4 = tk.Label(f4, image=background_image_4)
background_label_4.place(x=0, y=0, relwidth=1, relheight=1)


get_frame=tk.Canvas(f4,height=1,width=1)
get_frame.grid(row=2,column=2,padx=120,pady=150)
url_entered=tk.StringVar()
entry_2=tk.Entry(f4,width=40,textvariable=url_entered)
entry_2.configure(font="Calibri 30",background="#d2d5d6")
entry_2.grid(row=4,column=5,ipadx=10,ipady=10)
button_4=tk.Button(f4,text=" GET CONTENTS\t",height=3,width=20,font="Calibri 15",command=get_it_all)
button_4.grid(row=5,column=5,pady=30)

#contents of MASS EMAIL

background_image_3=ImageTk.PhotoImage(Image.open("massemail.jpg"))
background_label_3 = tk.Label(f3, image=background_image_3)
background_label_3.place(x=0, y=0, relwidth=1, relheight=1)

def browse(*args):
	file = tkFileDialog.askopenfile(parent=root,filetypes=[('Text document','*.txt')],mode='rb',title='Choose a file')
	lines=file.readlines()
	file_n.set(file.name)
	if file != None:
		data = file.read()
		file.close()
		print "I got %d bytes from this file." % len(data)
		file_entered.set(str(file.name))
	return file

def mail_dialog(*args):

	mail_d=tk.Toplevel()
	label_5=tk.Label(mail_d,text="Enter subject :  ")
	label_5.grid(row=1,column=1,pady=10)
	subject=tk.StringVar()
	subject_t=tk.Entry(mail_d,textvariable=subject,width=50)
	subject_t.grid(row=1,column=2,pady=10,ipady=2)
	label_6=tk.Label(mail_d,text="Enter content :  ")
	label_6.grid(row=3,column=1,pady=10)
	content=tk.Text(mail_d,width=50,height=10)
	content.grid(row=3,column=2,pady=10)
	file=open(file_n.get(),"r")
	lines=file.readlines()
	def mail(*args):
		for i in lines:
			i.replace("\\r","")
			i.replace("\\n","")
			print "sending to"+str(i)
			server = smtplib.SMTP('smtp.gmail.com', 587)
			server.starttls()
			server.login("temp6323@gmail.com", "sid9013630180")
			text = content.get("1.0",END)
			subject_text=subject.get()
			msg = 'Subject: %s\n\n%s' % (subject_text, text)
			server.sendmail("temp6323@gmail.com", i, msg)
			server.quit()
	button_9=tk.Button(mail_d,text="Send",command=mail,font="Calibri 13")
	button_9.grid(row=5,column=5,pady=20,padx=20)



email_frame=tk.Canvas(f3,height=1,width=1)
email_frame.grid(row=2,column=2,padx=120,pady=150)
file_entered=tk.StringVar()
entry_4=tk.Entry(f3,width=30,textvariable=file_entered)
entry_4.configure(font="Calibri 30",background="#d2d5d6")
entry_4.grid(row=4,column=5,ipadx=10,ipady=10,padx=10)
button_7=tk.Button(f3,text=" Browse\t",height=2,width=20,font="Calibri 13",command=browse)
button_7.grid(row=4,column=6)
button_8=tk.Button(f3,text=" SEND MAIL\t",height=3,width=20,font="Calibri 15",command=mail_dialog)
button_8.grid(column=6,row=5,pady=20)
file_n=tk.StringVar()
label_7=tk.Label(textvariable=file_n)

#contents of MASS FILE TRANSFER
'''
class SendThread(Thread):

    def __init__(self):
        Thread.__init__(self)
       	print "trying to start server"

    def run(self):

			sock.listen(5)
		for i in range(5):
			print "Waiting for incoming connections..."
			(conn, addr) = sock.accept()
			print 'Got connection from ',(addr)
			#filename=conn.recv(1024)
			formats=["mp4","mkv", "3gp","asf", "AVI", "DVR-MS", "FLV","mp3","wma","MIDI","mov","Ogg", "OGM", "WAV", "MPEG-2", "AIFF"]
			files=[]
			for i in formats:
				files=files+(glob.glob("*"+i))
			f_list=""
			for i in files:
				f_list=f_list+"-->  "+i+"\n"
			c_name=conn.recv(1024)
			print c_name
			connected.append(c_name)
			connected_str.set("server connected to "+str(connected))
			conn.send(f_list)
			print "sending "+f_list+"..."
			filename=conn.recv(1024)
			print "chosen file is "+str(filename)
			newthread = ClientThread(addr,conn,filename)
			newthread.start()
			threads.append(newthread)

file_entered2=tk.StringVar()
def send(*args):

	def browse2(*args):
			file = tkFileDialog.askopenfile(mode='rb',title='Choose a file')
			print file.name
			file_entered2.set(file.name)
			return file
	threads_send=[]
	host = '0.0.0.0'
	port=8000	
	sock=socket.socket()
	sock.bind((host,port))
	button_10.config(state="disabled")
	button_11.config(state="disabled")
	threads=[]
	clients=tk.Toplevel()
	def dest(*args):
		def send_all(*args):
			print "sending"+file_entered2.get()+"..."
			for i in connections:
				f1=open(file_entered2.get(),"rb")
				bytesToSend=f1.read(102400)
				while bytesToSend:
					i.send(bytesToSend)
					bytesToSend=f1.read(102400)
				f1.close()
		clients.destroy()
		sock.listen(int(num.get()))
		i=0
		connections=[]
		while(i!=int(num.get())):
			(conn, addr) = sock.accept()
			connections.append(conn)
			print "Got connection from "+str(addr)+ "..."
			i=i+1
		print "listened"
		s_file=tk.Toplevel()
		entry_6=tk.Entry(s_file,width=10,textvariable=file_entered2)
		entry_6.pack()
		button_13=tk.Button(s_file,text=" Browse\t",height=1,width=10,font="Calibri 13",command=browse2)
		button_13.pack()
		button_14=tk.Button(s_file,text="Send",command=send_all)
		button_14.pack()

	num=tk.StringVar()
	label_10=tk.Label(clients,text="please enter no of receivers")
	label_10.grid(row=1,column=1,padx=50)
	entry_5=tk.Entry(clients,textvariable=num)
	entry_5.grid(row=2,column=1,pady=10)
	button_12=tk.Button(clients,text="next",command=dest)
	button_12.grid(row=3,column=1,pady=20)
		
	sendthread=SendThread()
	sendthread.start()

def receive(*args):
	p = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	p.connect(('10.255.255.255', 0))
	IP = p.getsockname()[0]
	p.close()
	host=IP
	port=8000
	try:
		s=socket.socket()
		s.connect((host,port))
		button_1.config(state="disabled")
	except:
		tkMessageBox.showerror("Error","Server not started ..\n")
		print "server not started .."
	data=s.recv(1024)
	print "receiving"+file_entered2.get()
	f=open("new_",'wb')
	while data:
		f.write(data)
		data=s.recv(102400)
	print "received all"
	

background_image_2=ImageTk.PhotoImage(Image.open("filetransfer.jpg"))
background_label_2 = tk.Label(f2, image=background_image_2)
background_label_2.place(x=0, y=0, relwidth=1, relheight=1)
void2=tk.Frame(f2).grid(column=2,row=4,pady=80,padx=45)
button_10=tk.Button(f2,text="Send File",font="Calibri 15",width=30,command=send,height=2)
button_10.grid(column=4,row=10,pady=70,padx=370)
button_11=tk.Button(f2,text="Receive File",font="Calibri 15",command=receive,width=30,height=2)
button_11.grid(column=4,row=11)
label_9=tk.Label(f2,font="Calibri 15")
label_9.grid(column=4,row=18,pady=200)'''

root.bind('<Return>', openfile)
mainframe["bg"] = "#ffffff"
toplevel = root.winfo_toplevel()
toplevel.wm_state('zoomed')
root.title("PROP PROJECT :P")

root.mainloop()