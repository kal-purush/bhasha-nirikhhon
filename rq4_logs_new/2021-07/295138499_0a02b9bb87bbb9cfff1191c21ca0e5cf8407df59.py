import cv2
import numpy as np
def gcd(x, y):
  
   while(y):
       x, y = y, x % y
  
   return x
def modInverse(a, m):
    m0 = m
    y = 0
    x = 1
 
    if (m == 1):
        return 0
 
    while (a > 1):
 
        # q is quotient
        q = a // m
 
        t = m
 
        # m is remainder now, process
        # same as Euclid's algo
        m = a % m
        a = t
        t = y
 
        # Update x and y
        y = x - q * y
        x = t
 
    # Make x positive
    if (x < 0):
        x = x + m0
 
    return x
def expo(bas, exp,mod):
    if (exp == 0):
        return 1
    if (exp == 1):
        return bas % mod
    t = expo(bas, int(exp / 2),mod);
    t = (t * t) % mod
    if (exp % 2 == 0):
        return t
    else:
        return ((bas % mod) * t) % mod
def generate_key(prime1,prime2):
    mod=prime1*prime2
    totient=(prime1-1)*(prime2-1)
    
    public=0
    temp=totient//20 #For an ideal value of public key, if the public key is too close to totient, then public key and private key are very close, sometimes even same
    print(temp)
    for i in range(temp,1,-1):
        if(gcd(totient,i)==1):
            public=i
            break
    public=(public,mod)
    private=modInverse(public[0],totient)
    private=(private,mod)
    return public,private
def char_cryptanalyse(char,key):
    return expo(char,key[0],key[1])%key[1]
def read_data(image,flag):
    #flag 1 for colour
    #flag 0 for whitescale
    #flag -1 for inverted
    img=cv2.imread(image,flag)
    return img
def cryptanalyse_image(img,key):
    size=img.shape
    alterimage=np.zeros(size)
    if(len(size)==2):
        for i in range(size[0]):
            for j in range(size[1]):
                alterimage[i,j]=int(char_cryptanalyse(img.item(i,j),key))
    else:
        for i in range(size[0]):
            for j in range(size[1]):
                for k in range(size[2]):
                    alterimage[i,j,k]=int(char_cryptanalyse(img.item(i,j,k),key))
    return alterimage
def showimg(img):
    cv2.imshow("hehe",img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    cv2.waitKey(1)
def writeimg(name,img):
    cv2.imwrite(name+'.jpg',img)
def readfile(name):
    with open(name) as f:
        lines = f.read()
    return lines
def process(text):
    arr=np.zeros(len(text))
    for i in range(len(text)):
        arr[i]=ord(text[i])
    return arr
def cryptanalyse_text(arr,key):
    for i in range(arr.shape[0]):
        arr[i]=char_cryptanalyse(arr[i],key)
    return arr
def reprocess(arr):
    c=arr.astype(np.uint8)
    l=""
    for i in range(c.shape[0]):
        l+=(chr(c[i]))
    return l
def writefile(name,text):
    f = open(name, "w",encoding="utf-8")
    f.write(text)
    f.close()
