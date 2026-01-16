import numpy as np
from matplotlib import pyplot as plt
dp=0.8
ds=0.2
wp=2*np.pi*4000
ws=2*np.pi*5000
def butterworth(dp,ds,wp,ws):
    a=(1.0/(dp**2))-1
    b=(1.0/(ds**2))-1
    N=int(np.ceil(0.5*np.log10(a/b)/np.log10(wp/ws)))
    wc=wp/(a**(1.0/(2.0*N)))
    return N,wc
def chebyshev(dp,ds,wp,ws):
    a=(1.0/(dp**2))-1
    b=(1.0/(ds**2))-1
    eps=np.sqrt(a)
    N=int(np.ceil(np.arccosh(np.sqrt(b/a))/np.arccosh(ws/wp)))
    return N,eps

Nb,wc=butterworth(dp,ds,wp,ws)
print (Nb,wp,wc,ws)
W=2*np.pi*np.asarray(range(10000),'float32')
Hb=[]
for w in range(10000):
    mag=1.0/np.sqrt(1+((2*np.pi*w/wc)**(2.0*Nb)))
    Hb=np.append(Hb,mag)
print (Hb.shape)

a1=plt.subplot(211)
#a1.plot(Hb)
#a1.plot(20*np.log10(Hb))
a1.plot(np.log10(W),20*np.log10(Hb))
plt.ylabel('magitude/gain')
plt.title('Butterworth Analog Filter')

Nc,eps=chebyshev(dp,ds,wp,ws)
print (Nc,eps)
Hc=[]
for w in range(10000):
  if(2*np.pi*w<wp):
    mag=1.0/np.sqrt(1+(eps*(np.cos(Nc*np.arccos(2*np.pi*w/wp))))**2)
    Hc=np.append(Hc,mag)
  else:
    mag1=1.0/np.sqrt(1+(eps*(np.cosh(Nc*np.arccosh(2*np.pi*w/wp))))**2)
    Hc=np.append(Hc,mag1)

print (Hc.shape)
a2=plt.subplot(212,sharex=a1)
#a2.plot(Hc)
#a2.plot(20*np.log10(Hc))
a2.plot(np.log10(W),20*np.log10(Hc))
plt.ylabel('magitude/gain')
plt.xlabel('frequency in Hz')
plt.title('Chebyshev Analog Filter')
plt.show()