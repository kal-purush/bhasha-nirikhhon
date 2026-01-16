from __future__ import print_function
from leer_mrz import leer_mrz
import camerastream
#import obtenerfoto
import face_recognition
import cv2
import tkinter as tk
from PIL import Image, ImageTk
import threading
import datetime
from imutils.video import VideoStream
import imutils
import os
import time
import pygame
import pygame.camera
from pygame.locals import *

LARGE_FONT= ("Verdana", 14)
MEDIUM_FONT= ("Verdana", 12)
SMALL_FONT= ("Verdana", 10)

width, height = 800, 600
cap = cv2.VideoCapture(0)
cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)

def lectura_mrz():
    try:
        mrz = leer_mrz()
    except Exception as e:
        mrz=f"Error: No pudo leerse el MRZ correctamente. {e}"
    return mrz
    
def verificacion_de_cara():
    orig_foto = face_recognition.load_image_file('Imagenes_Cara/foto_cara_cedula_miguel.jpg')
    orig_encodings = face_recognition.face_encodings(orig_foto)[0]

    verificacion=False
    #camerastream.camstream()
    #vs=VideoStream(0).start()
    #vs=cv2.VideoCapture(0)
    pygame.init()
    pygame.camera.init()
    camera = pygame.camera.Camera('/dev/video0', (1280, 720))
    camera.start()
    
    PhotoBoothApp(camera, "./Imagenes_Cara")
    
    val_foto=face_recognition.load_image_file('Imagenes_Cara/image_miguel.jpeg')
    val_encodings = face_recognition.face_encodings(val_foto)[0]
        
    verificacion = face_recognition.compare_faces([orig_encodings], val_encodings)
    return verificacion

class PhotoBoothApp:
    def __init__(self, vs, outputPath):
        
        self.vs = vs
        self.outputPath = outputPath
        self.frame = None
        self.thread = None
        self.stopEvent = None
        
        self.root = tk.Tk()
        self.panel = None
        
        btn = tk.Button(self.root, text="Snapshot", command=self.takeSnapshot)
        btn.pack(side ="bottom", fill="both", expand ="yes", padx=10, pady=10)
        
        self.stopEvent = threading.Event()
        self.thread = threading.Thread(target=self.videoLoop, args=())
        self.thread.start()
        
        self.root.wm_title("PyImage")
        self.root.wm_protocol("WM_DELETE_WINDOW", self.onClose)
        
    def videoLoop(self):
        try:
            while not self.stopEvent.is_set():
                self.frame = self.vs.get_image()#read()
                self.frame = imutils.resize(self.frame, width=300)
                
                image = cv2.cvtColor(self.frame, cv2.COLOR_BGR2RGB)
                image = Image.fromarray(image)
                image = ImageTk.PhotoImage(image)
                
                if self.panel is None:
                    self.panel = tk.Label(image=image)
                    self.panel.image = image
                    self.panel.pack(side="left", padx=10, pady = 10)
                else:
                    self.panel.configure(image=image)
                    self.panel.image=image
        except RuntimeError as e:
            print("[INFO] caught a RuntimeError")
            
    def takeSnapshot(self):
        ts = datetime.datetime.now()
        filename= "{}.jpg".format(ts.strftime("%Y-%m-%d_%H-%M-%S"))
        p = os.path.sep.join((self.outputPath, filename))
        cv2.imwrite(p, self.frame.copy())
        print("[INFO] saved {}".format(filename))
        
    def onClose(self):
        print("[INFO] closing...")
        self.stopEvent.set()
        self.root.quit()

class LogIn(tk.Tk):

    def __init__(self, *args, **kwargs):
        
        tk.Tk.__init__(self, *args, **kwargs)
        container = tk.Frame(self)
        
        container.pack(side="top", fill="both", expand = True)

        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        self.frames = {}
        pages = [StartPage, PageOne, PageTwo]

        for F in pages:

            frame = F(container, self)

            self.frames[F] = frame

            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame(StartPage)

    def show_frame(self, cont):

        frame = self.frames[cont]
        frame.tkraise()

        
class StartPage(tk.Frame):

    def __init__(self, parent, controller):
        tk.Frame.__init__(self,parent)

        label = tk.Label(self, text="\nBuenos dias.\nPara comenzar, ingrese la cedula y Presione Enter.", font=LARGE_FONT)
        label.pack(pady=10,padx=10)

        button = tk.Button(self, text="Enter", font=LARGE_FONT,
                            command=lambda: controller.show_frame(PageOne))
        button.pack()


class PageOne(tk.Frame):
    
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label1 = tk.Label(self, text="Leyendo MRZ...", font=LARGE_FONT)
        label1.pack(pady=10,padx=10)
        
        button3 = tk.Button(self, text="Leer Cedula",
                            command= lambda: self.actualizar(label1, label2, label3))
        button3.pack()
        
        label1 = tk.Label(self, text="", font=LARGE_FONT)
        label1.pack(pady=10,padx=10)
        label2 = tk.Label(self, text='', font=MEDIUM_FONT)
        label2.pack(pady=10,padx=10)
        label3 = tk.Label(self, text='', font=SMALL_FONT)
        label3.pack(pady=10,padx=10)
        
        button1 = tk.Button(self, text="Ok",
                            command=lambda: controller.show_frame(PageTwo))
        button1.pack()
        
        button2 = tk.Button(self, text="Volver a empezar",
                            command=lambda: controller.show_frame(StartPage))
        button2.pack()
        
    def actualizar(self, label1, label2, label3):
        mrz = lectura_mrz()
        label1 = label1.config(text="MRZ:")
        label2 = label2.config(text=mrz)
        label3 = label3.config(text="Si es correcto presione Ok, sino vuelva a comenzar")


class PageTwo(tk.Frame):

    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label4 = tk.Label(self, text="Verificación Facial", font=LARGE_FONT)
        label4.pack(pady=10,padx=10)

        label5 = tk.Label(self, text=f"Estamos leyendo la imagen de tu cedula, por favor espere...", font=SMALL_FONT)
        label5.pack(pady=10,padx=10)
        
        button5 = tk.Button(self, text="Sacar Foto!",
            command=lambda: self.actualizar(label6))
        button5.pack()
        

        label6 = tk.Label(self, text="", font=SMALL_FONT)
        label6.pack(pady=10,padx=10)

        button4 = tk.Button(self, text="Continuar",
            command=lambda: controller.show_frame(StartPage))
        button4.pack()
    
    def actualizar(self, label6):
        verificacion = verificacion_de_cara()
        if verificacion:
            label6 = label6.config(text="Okay! Gracias por tu tiempo, por favor pasa a la cabina Numero 1")
        else:
            label6 = label6.config(text="Lamentamos informarle que no logramos identificarte, por favor vuelva a comenzar o pida asistencia.")

app = LogIn()
app.mainloop()