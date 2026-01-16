from pytube import YouTube
from tkinter import *
import tkinter.ttk as ttk
import threading, sys, os.path

class YouTubeDownloder():
    def __init__(self):
        self.tube = Tk()
        self.tube.iconbitmap(self.resource_path("sim\\calculator-ico.ico"))
        self.tube.title("Tube Loader")
        self.toplink = Frame(self.tube, bg="brown")
        self.toplink.pack(side = "top", expand = YES, fill = BOTH)
        self.toplink.grid_propagate(0)
        Label(self.toplink, text = "Enter video url here", font=('arial',10), bg="brown", foreground="white" ).pack(side="left")
        self.urllink = Entry(self.toplink, width = 80, bd=5, insertwidth=4, justify='left', font=('arial',10))
        self.urllink.pack(side="left")
        self.textFrame = Frame(self.tube, bg="brown")
        self.textFrame.pack(side = "top", expand = YES, fill = BOTH)
        self.textFrame.grid_propagate(0)
        self.comment= Text(self.textFrame, width=100, height=30, wrap=WORD)
        self.comment.pack(side="left")
        self.vscroll = Scrollbar(self.textFrame, orient = "vertical", command = self.comment.yview)
        self.vscroll.pack(side= "left", fill=BOTH)
        self.button = Frame(self.tube, bg="brown")
        self.button.pack(side = "top", expand = YES, fill = BOTH)
        self.button.grid_propagate(0)
        self.info = Label(self.button, text="", bg="brown", foreground="white")
        self.yt = " "
        self.progressbar = ttk.Progressbar(self.button, orient=HORIZONTAL, mode="indeterminate", maximum=100)
        self.bdetail = Button(self.button, width = 10, text = "view details", foreground="white", bg="black", command =self.details)
        self.bdetail.pack(side="left", padx=10)
        self.dload = Button(self.button, width = 10, text = "Download", foreground="white", bg="black", command =self.download)
        self.dload.pack(side="left", padx=10)
        
        self.tube.mainloop()

    def resource_path(self, relative_path):
        """Get the absolute path to the resource, works for dev and for Pyinstaller"""
        try:
            # PyInstaller create a temp folder and stores path in _MEIPASS
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    def downloadThread(self):
        self.info.destroy()
        try:
            self.dload.config(state=DISABLED, bg='red')
            self.yt = YouTube(self.urllink.get())
            self.stream = self.yt.streams.get_highest_resolution()
            self.stream.download(os.path.expanduser("~")+"\\Downloads")
            self.pb_stop()
            self.info.destroy()
            self.info = Label(self.button, text="Download completed!!", bg="brown", foreground="white")
            self.info.pack(side="left")
            self.dload.config(state=NORMAL, bg='black')
        except:
            self.pb_stop()
            self.info.destroy()
            self.info = Label(self.button, text="URL fails, check your url or network setting", bg="brown", foreground="white")
            self.info.pack(side="left")
            self.dload.config(state=NORMAL, bg='black')

    def download(self):
        self.downloads = threading.Thread(target=self.downloadThread, name='download', daemon=True)
        self.downloads.start()
        if self.downloads.isAlive():
            self.pb_start(10)
            self.info = Label(self.button, text="Wait while getting your video downloaded ...", bg="brown", foreground="white")
            self.info.pack(side="left")

    def detailsThread(self):
        self.info.destroy()
        try:
            self.bdetail.config(state=DISABLED, bg='red')
            self.comment.delete(1.0, 'end')
            self.yt = YouTube(self.urllink.get())
            self.title = "Title : "+ self.yt.title
            # To get number of views
            self.views = "Views : "+ str(self.yt.views)
            # To get the length of video
            self.duration = "Duration : "+ str(self.yt.length)
            # To get description
            self.description = "Description : "+ self.yt.description
            # To get ratings
            self.rating = "Ratings : "+ str(self.yt.rating)
            self.videoInfo = "Your video details are as follows:\n {} \n {} \n {} \n {} \n {}"
            self.comment.insert("1.0", self.videoInfo.format(self.title, self.views, self.duration, self.description, self.rating, "end"))
            self.pb_stop()
            self.info.destroy()
            self.info = Label(self.button, text="details completed!!", bg="brown", foreground="white")
            self.info.pack(side="left")
            self.bdetail.config(state=NORMAL, bg='black')
            
        except:
            self.pb_stop()
            self.info.destroy()
            self.info = Label(self.button, text="URL fails, check your url or network setting", bg="brown", foreground="white")
            self.info.pack(side="left")
            self.bdetail.config(state=NORMAL, bg='black')

    def details(self):
        self.detail = threading.Thread(target=self.detailsThread, name='details', daemon=True)
        self.detail.start()        
        if self.detail.isAlive():
            self.pb_start(10)          
            self.info = Label(self.button, text="Wait while getting your details downloaded ...", bg="brown", foreground="white")
            self.info.pack(side="left")

    def pb_start(self, val):
        """ starts the progress bar """
        self.progressbar = ttk.Progressbar(self.button, orient=HORIZONTAL, mode="indeterminate", maximum=100)
        self.progressbar.pack(side="left")
        self.progressbar.start(val)

    def pb_stop(self):
        """ stops the progress bar """
        self.progressbar.stop()
        self.progressbar.destroy()

if __name__ == "__main__":
    youtube = YouTubeDownloder()