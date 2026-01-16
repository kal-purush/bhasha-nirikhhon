import time
import RPi.GPIO as GPIO
GPIO.setmode(GPIO.BOARD)   #
GPIO.setwarnings(False)

GPIO.setup(8, GPIO.OUT) # pasos 2
GPIO.setup(10, GPIO.OUT) #direccion 2
GPIO.setup(11, GPIO.OUT) #pasos  1 s
GPIO.setup(13, GPIO.OUT) #direccion 1 s
GPIO.setup(15, GPIO.OUT) #enable s
GPIO.setup(16, GPIO.OUT) #selenoide
GPIO.setup(33, GPIO.OUT) #servo motor 



servo=GPIO.PWM(33,50)
servo.start(0)

GPIO.output(15, 0)

class dibujar:
    def init(self,ppmmx,ppmmy):
        GPIO.output(15,0)
        self.Z=10
        self.Za=self.Z
        self.decimalx,self.decimaly=0,0
        self.tools=[0,0,0]# valores de pwm que acomodan la herramienta
        self.ppmmy=ppmmy
        self.ppmmx=ppmmx
        print("los ppmm estan definidas",self.ppmmx,self.ppmmy)
        
        self.home()        
        self.decimal_acumuladoX,self.decimal_acumuladoY=0,0
        
    def __init__(self,ppmmx, ppmmy):#ppmm se usara para calcular la cantidad de pasos que deve de dar por medida
       self.init(ppmmx,ppmmy)
       
    def home(self):
        # puedes poner un una funcion para los limites de carrera aqui
        self.x,self.y,self.z=0,0,0#aqui se define la posicion inicial de la herramienta
        self.xa,self.ya,self.za=self.x,self.y,self.z
        self.puntoX,self.puntoXa,self.puntoY,self.puntoYa=0,0,0,0
        print("coordenadas iniciales definidas")
        
    def moverLinea(self,puntos):
        for punto in puntos:
            self.puntoX=int(punto[0])
            self.puntoY=int(punto[1])
            
            self.girarMotorY(self.puntoY-self.puntoYa)
            self.girarMotorX(self.puntoX-self.puntoXa)
            
            self.puntoXa=self.puntoX
            self.puntoYa=self.puntoY
    def girarMotorX(self,pasos):
        if pasos<0:
            Direccion=0

        else:
            Direccion=1
        GPIO.output(10,Direccion)
        GPIO.output(13,Direccion)
     #
        for i in range(abs(pasos)):
            #print("X")        
            time.sleep(0.003)
            GPIO.output(11, 1)
            time.sleep(0.003)
            GPIO.output(11, 0)
        #
    def girarMotorY(self,pasos):
        if pasos<0:
            Direccion=0
        else:
            Direccion=1
        GPIO.output(10,Direccion)
        GPIO.output(13,Direccion)
        for i in range(abs(pasos)):
            #print("Y")
            time.sleep(0.003)
            GPIO.output(8, 1)
            time.sleep(0.003)
            GPIO.output(8, 0)
        #
        
    def bresenham(self,x0,y0,x1,y1):
        self.puntos=[]
        dx=x1-x0
        dy=y1-y0
        if dy<0:
            dy=-dy
            stepy=-1
        else:
            stepy=1
        if dx<0:
            dx=-dx
            stepx=-1
        else:
            stepx=1
        x=x0
        y=y0
        #pprint(x,y)
        self.puntos.append([x,y])
        if dx>dy:
            p=dy-dx
            incE=2*dy
            incNE=2*(dy-dx)
            while x!= x1:
                x=x+stepx
                if p<0:
                    p=p+incE
                else:
                    y=y+stepy
                    p=p+incNE
                #pprint(x,y)
                self.puntos.append([x,y])
        else:
            p=2*(dx-dy)
            incE=2*dx
            incNE=2*(dx-dy)
            while y!=y1:
                y=y+stepy
                if p<0:
                    p=p+incE
                else:
                    x=x+stepx
                    p=p+incNE
                #pprint(x,y)
                self.puntos.append([x,y])
        self.moverLinea(self.puntos)
    def zeta(self,zeta):
        
        self.Z=zeta
        if self.Z!=self.Za:
            if self.Z <=0:
                GPIO.output(16,0)#el selenoide.
                time.sleep(.3)
            elif self.Z>0:
                GPIO.output(16,1)#el selenoide.
                time.sleep(1)
        self.Za=self.Z
    
    def toolChange(self,ToNum):
        #Currently nothing here
        pass
        
        

    def leerGcode(self,nombre):
        GPIO.output(15,0)
        self.nombre=nombre
        self.archivo= open(self.nombre,mode="r")
        con=0
        for line in self.archivo:
            con+=1
            l=line[0:3]
            if l== "M06":
                c=0
                print ("tool change")
                for i in line:
                    c+=1
                    if i=="T" or i=="t":
                        self.tool=int(line[c+1:c+3])
                        print(self.tool)
                       
                
            if l == "G00"or l =="G01":
                
                c=0
                print (line)
                for i in line:
                    c+=1
                    if i =="Z" or i =="z":
                        self.z=float(line[c+1:c+4])
                        self.zeta(self.z)
                        #print("z",self.z)

                    if i =="Y" or i =="y":
                        self.y=float(line[c+1:c+6])
                        #pprint("y",self.y)
                    if i =="X" or i=="x":
                        self.x=float(line[c+1:c+6])
                
                resy= self.y*self.ppmmy+self.decimaly
                self.decimaly= resy-int(resy)
                BresY=int(resy)
                
                resx= self.x*self.ppmmx+self.decimalx
                self.decimalx= resx-int(resx)
                BresX=int(resx)
                self.bresenham(self.xa,self.ya,BresX,BresY)# aqui no se le pueden dar redondeados.
                self.xa=BresX
                self.ya=BresY
                self.za=self.z
        print("termino")
        
        GPIO.output(16,0)
        self.init(4,-4.7)
        GPIO.output(15,1)
            
print("start")
nuevo=dibujar(4,-4.7)#35 de diametro, #0.054006
nuevo.leerGcode("/home/pi/Desktop/code/python/myPloter/cubo.ngc")