import numpy as np

#Scipy
from scipy.signal import hilbert
from scipy.fftpack import fft, fftfreq
from scipy.signal import find_peaks

#Image proccesing

from skimage.transform import resize
from PIL import Image
import tifffile as tf

# Path libraries

from os import listdir
from pathlib import Path 

# csv reader
import csv



def napari_read_tiff(pathlibpath,start=0, folder=False, nframes='all'):
    '''
    Read a multiframe tif image and loads it in a numpy array
    '''
    
    if not folder:
        with Image.open(str(pathlibpath), mode='r') as img:
            stack = []
            if nframes == 'all':

                try:
                    for i in range(start,img.n_frames):
                        img.seek(i)
                        stack.append(np.array(img))
                except:
                    print(i)
                    pass
        #     else:
        #         for i in range(start,nframes):
        #             img.seek(i)
        #             stack.append(np.array(img))
    if folder:
        files = listdir(pathlibpath)
        stack = []
        for file in files:
            filepath = pathlibpath / file
            with Image.open(str(filepath),mode='r') as img:
                stack.append(np.array(img))
    return np.transpose(np.array(stack), (1, 2, 0))


def write_tiff(vol,path_to_save):

    if len(vol.shape) == 3:

        vol = np.transpose(vol,(2,0,1))

    tf.imwrite(path_to_save,vol)

class FFTAnalyze:
    
    def __init__(self):
        
        self.ref = None #Reference signal ndarray 1D
        self.ref_loaded = False #Control var, True if reference signal is loaded
        self.left = 0
        self.right = 0
        
    def set_ref_window(self,signal ,left ,right): #Set reference signal window
        
        self.left = left
        
        self.right = right
        
        self.ref = np.zeros(signal.shape) #reference 0s signal
        
        self.ref[left:right] = signal[left:right] #Copying window values from signal to reference
        
        self.ref_loaded = True 
        
        return self.ref 
    
    def get_ref(self): #Returns reference signal if already computed
        
        if not self.ref_laoded :
            
            print("No reference signal computed")
            
            return None
        
        return self.ref
    
    def backwall_window_volume(self,volume,lam_thickness,c_mat,fs): #backwall window 3d
        
        return np.apply_along_axis(self.backwall_window,2,volume,lam_thickness,c_mat,fs)
    
    
    def backwall_window(self,signal,lam_thickness,c_mat,fs):#signal should be analityc
        
        env = np.abs(signal) #envelope
        
        #First we get the window
        
        arg_max = np.argmax(env)  #Materials entry
        fin_mat = np.int( arg_max + lam_thickness * fs/ c_mat ) #theorical Materials exit
        
         # Returns the real materials exit
        
        if (fin_mat > 500):
            
            x_out = 0
        
        else:
           
            rango = range(fin_mat-5,fin_mat+5) #range around materials exit
            x_out = (fin_mat-5+np.argmax(env[rango])) # materials exit - 5 + windows max index
            
        
        # slicing to get the window
        l = len(signal) #signals len
        
        if (x_out != 0): # if is not back-wall
            
            result = np.zeros(shape=signal.shape) #zeros shaped as signal
            
            result[self.left:self.right] = signal[int(x_out-np.floor((self.right-self.left)/2)):int(x_out+np.floor((self.right-self.left)/2))] #copy window from signal to result
            
            return result
        
        if (x_out == 0): #means is back-wall due to arg_out func
            
            return np.zeros(shape=self.ref.shape) # zeros shaped as signal
        
        
        
    def backscattered_window_volume(self,volume,mask1,mask2):
        
        return np.apply_along_axis(self.backscattered_window,2,volume,mask1,mask2)
        
        
    def backscattered_window(self,signal,mask1,mask2): #Gets signal windowed in backscattered
        
        cropped = np.zeros(shape=signal.shape)
        
        cropped[mask1:mask2] = signal[mask1:mask2]
        
        return cropped 
    
    
    def fourier_volume(self,volume,fs): #signal must be windowed
    
        frec_vec = fftfreq(self.ref.size, d=1/fs*1e6)
        
        fft_ref = np.abs(fft(self.ref)) 
        
        fft_signal = np.abs(np.apply_along_axis(fft,2,volume))
        
        return frec_vec,fft_ref,fft_signal
    
    def fourier(self,signal,fs): #signal must be windowed
        
        frec_vec = fftfreq(self.ref.size, d=1/fs*1e6)
        
        fft_ref = np.abs(fft(self.ref))
        
        fft_signal = np.abs(fft(signal)) 
        
        return frec_vec,fft_ref,fft_signal
        
    
    def normalize(self,signal):
        
        return signal / np.abs(fft(self.ref))
        

class AspectRatio:
    
    def __init__(self):
        
        pass
    
    def ratio(self,x,y,x1,y1,axis = -1):
        
        #Computes new shape for the image where x and y are real measures of the object 
        #x1,y1 are unit per pixel ratios of each dimension in the image
        
        if x1 >= y1:
            
            axis = 0
            
            axis2 = 1
        
        else:
            
            axis = 1
            
            axis2 = 0
        
        sample = [x,y] #sample shape
        image = [x1,y1] #image shape
    
        
        sample_ratio = sample[axis]/sample[axis2]
        image_ratio = image[axis]/image[axis2]
        new_ratio = sample_ratio / image_ratio
        
        if axis: # axis = 1

            return (round(x1/new_ratio),y1)
        
        else:
            
            return (x1,round(y1/new_ratio))
        
    def reshape(self,x,y,image): # x y
        
        if len(image.shape) == 2:
        
            x1 = image.shape[0]
            y1 = image.shape[1]

            new_shape = self.ratio(x,y,x1,y1)

            return resize(image, new_shape,anti_aliasing=True,preserve_range=True) 
        x1 = image.shape[0]
        y1 = image.shape[1]
        z1 = image.shape[2]

        new_shape = self.ratio(x,y,x1,y1)

        return resize(image, new_shape + (z1,),anti_aliasing=True,preserve_range=True)


class RfAnalyze: 
    
    def __init__(self,data=[], save = True, fs = 40,axis = 2):

        if len(data):
    
            data = data.astype('int32')
            media = np.mean(data)
            data = data - media   
        
        self.data = data
        self.fs = fs
        self.hilbert = []
        self.saved = False
    
    def signal(self,data):

        data = data.astype('int32')
        media = np.mean(data)
        data = data - media 
        self.data = data
        self.hilbert = []
        self.saved = False


    
    def analytic(self,save = True):
        
        if save:
            self.saved = save
            self.hilbert = hilbert(self.data)
            return self.hilbert
        
        return hilbert(self.data)
            
    
    def envelope(self):
        
        if self.saved:
            print("Using saved")
            
            data = self.hilbert
        
        else:
            
            data = self.analytic(False)
            
        result = np.abs(data)
        
        return result
    
    def inst_phase(self):
        
        if self.saved:
            print("Using saved")
            
            data = self.hilbert
        
        else:
            
            data = self.analytic(True)
        
        def func(x): 
            return (np.angle(x))
        
        result = np.apply_along_axis(func,2,data)

        return result
    
    def inst_freq(self):
        
        if self.saved:
            print("Using saved")
            
            data = self.hilbert
        
        else:
            
            data = self.analytic(True)
        
        def func(x):
            
            return (np.diff(np.unwrap(np.angle(x))) / (2.0*np.pi) * self.fs)
        
        result = np.apply_along_axis(func,2,data)
        
        return result
    
class RfAligner:
    
    def __init__(self):
        
        self.rf = RfAnalyze()
        
    def align(self,rfsignal,alignment_ref,min_ref, analitycal_input = False):
        
        return np.apply_along_axis(self.align1D,2,rfsignal,alignment_ref,min_ref,analitycal_input)

    def envelope_align(self,rfsignal,alignment_ref,min_ref, analitycal_input = False):
        
        return np.apply_along_axis(self.envelope_align1D,2,rfsignal,alignment_ref,min_ref, analitycal_input)
        
    def align1D(self,rfsignal,alignment_ref,min_ref, analitycal_input = False):

        self.rf.signal(rfsignal)
        
        if analitycal_input:

            analytical = rfsignal
        
        else:

            analytical = self.rf.analytic()
        
        
        envelope = np.abs(analytical)


        array1d = rfsignal
        align = np.zeros(array1d.shape) #array de ceros con la shape de la señal
        maxi = np.argmax(array1d)
        pad = maxi - alignment_ref #distancia a la referencia


        # Si es fondo no se alinea

        longitud = len(envelope)

        final = int(longitud * 0.75)

        if final < maxi:

            return rfsignal

        if (pad > 0):

            if np.argmax(envelope)>min_ref: #Si el pico del envelope esta mas a la derecha que el low limit
                end = len(array1d[pad:]) #Selecciona los valores de pad en adelante y cuenta la longitud
                align[:end] = array1d[pad:] #Deja un hueco de 0s con el tamaño de pad y rellena el resto con la señal
                return align
            else:
                return rfsignal

        elif(pad<0):

            start = len(array1d[:pad]) #Selecciona los valores hasta el pad y cuenta la longitud

            align[np.abs(pad):] = array1d[:start] #Deja un hueco de 0s con el tamaño de pad al principio y rellena el resto con la señal

            return align

        else:

            return rfsignal
        
    
    def envelope_align1D(self,rfsignal,alignment_ref,min_ref, analitycal_input = False):
        
        self.rf.signal(rfsignal)
        
        if analitycal_input:

            analytical = rfsignal
        
        else:

            analytical = self.rf.analytic()
        
        analytical = self.rf.analytic()
        envelope = np.abs(analytical)

        array1d = envelope
        align = np.zeros(array1d.shape) #array de ceros con la shape de la señal
        maxi = np.argmax(array1d)
        pad = maxi - alignment_ref #distancia a la referencia


        # Si es fondo no se alinea

        longitud = len(envelope)

        final = int(longitud * 0.75)

        if final < maxi:

            return rfsignal

        if (pad > 0):

            if np.argmax(envelope)>min_ref: #Si el pico del envelope esta mas a la derecha que el low limit
                end = len(array1d[pad:]) #Selecciona los valores de pad en adelante y cuenta la longitud
                align[:end] = array1d[pad:] #Deja un hueco de 0s con el tamaño de pad y rellena el resto con la señal
                return align
            else:
                return rfsignal

        elif(pad<0):

            start = len(array1d[:pad]) #Selecciona los valores hasta el pad y cuenta la longitud

            align[np.abs(pad):] = array1d[:start] #Deja un hueco de 0s con el tamaño de pad al principio y rellena el resto con la señal

            return align

        else:

            return rfsignal


class gateManager:

    def __init__(self):

        pass

    def ifGate(self,signal,ff1,ff2,method = "max",lim = 0):

        boundaries = [ff1,ff2]
        
        window = signal[boundaries[0]:boundaries[1]]

        if method == "max":

            result = window.max()

            if result >= lim:

                return result

            return 0
        
        elif method == "first":

            peaks, values = find_peaks(window,height=lim)

            return values['peak_heights'][0]


    def negativeGate(self,signal,ff1,ff2,method = "max",lim = 0):

        boundaries = [ff1,ff2]
        
        window = signal[boundaries[0]:boundaries[1]]

        def fun(x):

            if (x > lim):

                return 0
            
            return x

        limiter = np.vectorize(fun)

        window = limiter(window)

        if method == "max":

            return window.max()
        
        elif method == "first":

            peaks, values = find_peaks(window, height=0)

            return values['peak_heights'][0]


class CscanManager:

    def __init__(self):
        
        self.gate1 = []
        self.gate2 = []
        self.gate3 = []

    def define_gate1(self,left,right,method = "max", lim = 0,type = 0):

        self.gate1 = [left,right,method,lim,type]
    
    def define_gate2(self,left,right,method = "max", lim = 0,type = 0):

        self.gate2 = [left,right,method,lim,type]
    
    def define_gate3(self,left,right,method = "max", lim = 0,type = 0):

        self.gate3 = [left,right,method,lim,type]
    

    def compute(self,data):

        gm = gateManager()

        if not self.gate1[4]:
            
            peak1 = gm.ifGate(data,self.gate1[0],self.gate1[1],lim=self.gate1[3],method=self.gate1[2])

        else:

            peak1 = gm.negativeGate(data,self.gate1[0],self.gate1[1],lim=self.gate1[3],method=self.gate1[2])
        
        
        if not peak1:

            return 0


        if not self.gate2[4] :

            peak2 = gm.ifGate(data,self.gate2[0],self.gate2[1],lim=self.gate2[3],method=self.gate2[2])
        
        else:

            peak2 = gm.negativeGate(data,self.gate2[0],self.gate2[1],lim=self.gate2[3],method=self.gate2[2])
            
        return ((10 *  np.log10(np.divide(peak2, peak1)+0.0001)) *(-2))
    
    def compute_3(self,data):

        gm = gateManager()

        if not self.gate1[4] :
            
            peak1 = gm.ifGate(data,self.gate1[0],self.gate1[1],lim=self.gate1[3],method=self.gate1[2])

        else:

            peak1 = gm.negativeGate(data,self.gate1[0],self.gate1[1],lim=self.gate1[3],method=self.gate1[2])
        
        if not peak1:

            return 0

        if not self.gate3[4] :
            
            peak3 = gm.ifGate(data,self.gate3[0],self.gate3[1],lim=self.gate3[3],method=self.gate3[2])

        else:

            peak3 = gm.negativeGate(data,self.gate3[0],self.gate3[1],lim=self.gate3[3],method=self.gate3[2])
            
        return ((10 *  np.log10(np.divide(peak3, 100)+0.0001)) *(-2))

    def compute_DAC(self,data): # La atenuacion DAC es pico de salida respecto a su calibracion (en nuestro caso 80%)

        gm = gateManager()

        if not self.gate1[4] :
            
            peak1 = gm.ifGate(data,self.gate1[0],self.gate1[1],lim=self.gate1[3],method=self.gate1[2])

        else:

            peak1 = gm.negativeGate(data,self.gate1[0],self.gate1[1],lim=self.gate1[3],method=self.gate1[2])
        
        
        if not peak1:

            return 0


        if not self.gate2[4] :

            peak2 = gm.ifGate(data,self.gate2[0],self.gate2[1],lim=self.gate2[3],method=self.gate2[2])
        
        else:

            peak2 = gm.negativeGate(data,self.gate2[0],self.gate2[1],lim=self.gate2[3],method=self.gate2[2])
            
        return ((10 *  np.log10(np.divide(peak2, 80)+0.0001)) *(-2))