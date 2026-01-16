import numpy as np
import matplotlib.pyplot as plt

data=np.loadtxt("sample.dat")
mu=np.mean(data)
sigma=np.std(data)
x = np.linspace(np.min(data),np.max(data),1000)
teo=(1.0/np.sqrt(2.0*np.pi*sigma**2)) * np.exp(-((x-mu)**2)/(2.0*sigma**2))


plt.figure(figsize=(18,9))
_=plt.hist(data, density=True, bins=250)
plt.plot(x, teo)
plt.xlim([np.min(data),np.max(data)])
plt.xlabel('x')
plt.ylabel('PDF(x)')
plt.savefig("sample.pdf")