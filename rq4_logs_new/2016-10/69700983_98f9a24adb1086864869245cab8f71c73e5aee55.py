#coding=utf-8

import os,platform,sys
from PIL import Image
import pandas as pd
import numpy as np
import multiprocessing as mp




def get_terminal_bound():
    rows,columns=os.popen('stty size', 'r').read().split()
    width = int(columns)
    height = int(rows)
    return (int(rows),int(columns))

class TestPlatform:
    def __init__(self):
        self.python_version = platform.python_version() # Python version
        self.platform_architecture = platform.architecture()#programme structure('32bit', 'WindowsPE')
        self.node = platform.node() # name
        self.platform_version=platform.version()#  capture operator system's core version
        self.platform_name_version = platform.platform()#capture operator system,Windows-7-6.1.7601-SP1
        self.processor = platform.processor()#computer processor info,'Intel64 Family 6 Model 42 Stepping 7, GenuineIntel'
        self.python_compiler=platform.python_compiler()#
        self.otherinfo=platform.uname()#  other
        self.platform_system=platform.system()#system
    def UsePlatform(self, returnornot=False):
        sysstr = self.platform_system
        if returnornot:
            return sysstr
        else:
            pass
        #if(sysstr =="Windows"):
        #    return ("Call Windows tasks")
        #elif(sysstr == "Linux"):
        #    print ("Call Linux tasks")
        #elif(sysstr == "Darwin"):
        #     print ("Call Darwin or Mac tasks")
        #else:
        #    print ("Other System tasks")




    #print

    #print platform.system_alias()

    #print












        #return (rows,columns)
    #def get_terminal_width_resize():
    #    c = commands.getoutput('resize').split('\n')
    #    c = [x for x in c if x.startswith('COLUMNS=')]
    #    if c:
    #        c = c[0]
    #        dummy, c = c.split('=', 1)
    #        if c[-1] == ';':
    #            c = c[:-1]
    #    if c:
    #        return int(c)
    #    else:
    #        return None

    #def get_terminal_height():
    #    c = commands.getoutput('resize').split('\n')
    #    c = [x for x in c if x.startswith('LINES=')]

    #    if c:
    #        c = c[0]
    #        dummy, c = c.split('=', 1)
    #        if c[-1] == ';':
    #            c = c[:-1]
    #    if c:
    #        return int(c)
    #    else:
    #        return None

class show_terminal_bound:
    def __init__(self):
        self.name='show_bound_process'
        rows,columns=os.popen('stty size', 'r').read().split()
        #self.width = 20
        #self.height = 50
        self.width = int(columns)
        self.height = int(rows)
        self.system= platform.system()#system




class pic2symboltext:
    def __init__(self):
        self.name='pic2text process'
        rows,columns=os.popen('stty size', 'r').read().split()
        self.width = int(columns)
        self.height = int(rows)
        self.system= platform.system()#system
        self.symboldataset=None


    def preprocess(self,img_name):

        img = Image.open(img_name)
        img_w, img_h = img.size
        #img_m = max(img.size)
        windows_w=self.width
        windows_h=self.height
        img = img.resize((w,h))
        #img = img.convert('L')
        'resize picture process'


        return img

    def pandalizationfortest(self,character_image):
        pix=character_image.load()
        self.symboldataset=np.zeros((character_image.size[1],character_image.size[0]))
        data=self.scan_rep(pix,character_image)
        dataset=pd.DataFrame(data)
        #print datas
        self.symboldataset=dataset
        return dataset

        #data[0][1]=1
    def scan_rep(self,pix,character_image):
        data=self.symboldataset
        for i in xrange(character_image.size[0]):
            for j in xrange(character_image.size[1]):
                data[j,i]=pix[i,j][0]
                #if data[i][j]<10:
                #    data[i][j]=0
                #else:
                #    data[i][j]=1



        return data


    #def create_symbol_text(img):
    #    pix = img.load()
    #    pic_str = ''
    #    width, height = img.size
    #    pandalizationfortest(img



                    #pic_str += color[int(pix[w,h]) * 14 /255]
                #pic_str += '\n'
        return pic_str



    def printdash(self):
        length= self.width
        h=self.height
        for x in xrange(h):
            sys.stdout.write ('*'*length)
            sys.stdout.flush()



class checkbmp2png:
    def __init__(self):
        self.n=''
        self.name=''
        self.ext=''
        self.status=''


    def testbmp(self,bmp_img_name):
        name,ext = os.path.splitext(bmp_img_name)
        if ext.lower() == '.bmp':
            self.n=bmp_img_name
            self.name=name
            self.ext=ext.lower()
        else:
            pass

        return self.ext
    def bmp2png(self,bmp_img_name):
        self.testandbmp2png(bmp_img_name)
        if self.ext=='.bmp':
            self.ext=='.png'
            self.status='changed'
        else:
            self.status='unchanged'
        pass








def getDirList( p ):
        p = str( p )
        if p=="":
              return [ ]
        #p = p.replace( "/","/")
        if p[-1] != "/":
             p = p+"/"
        a = os.listdir( p )
        b = [ x   for x in a if os.path.isdir( p + x ) ]
        return b

def scan_all_pic_file_process(rdir):
    fo=os.walk(rdir)
    f=[]
    for (root,subfolder,files_a) in fo:
        pass
    for file_a in files_a:
        full_path_for_each_bmp=(root,file_a)
        f.append(full_path_for_each_bmp)
        #print root
        #num=root.lstrip(rdir)
        #print num,type(num)

    return f


#def multi_scan(img):
#    pool = mp.Pool()
    #for rdir in rdirs:
#    yv=pool.map


def bmp2png(f):
    path=f[0]
    name=f[1]
    f_bmp=path+'/'+name
    f_name,f_ext = os.path.splitext(f_bmp)
    if f_ext =='.bmp':
        f_bmp_png=path+'_png/'+name
        f_png_name,f_png_ext = os.path.splitext(f_bmp_png)
        f=f_png_name+'.png'
        Image.open(f_bmp).save(f)
    else:
        pass

if __name__=='__main__':
    img_name='/Users/admin/Documents/python/personal_terminal_demo/demo/test/0052.bmp'
    img=Image.open(img_name)
    s=pic2symboltext().pandalizationfortest(img)
    test='/Users/admin/Documents/python/personal_terminal_demo/demo/video2picture/Capture'
    ff=scan_all_pic_file_process(test)
    #ff=[]
    #ff.append(fff[199])
    #ff.append(fff[100])

    #print len(ff),ff[0][0]+'/'+ff[0][1]
    pool = mp.Pool()
    #for rdir in rdirs:
    pool.map(bmp2png,ff)














    def bmptopng(saving_path,filename):
        if ext.lower() == '.bmp':
            target= saving_path+filename
            Image.open(bmp_img_name).save(target)
        else:
            pass











    #a=TestPlatform()
    #print a.platform_system
    #v.printdash()
    #print g.height