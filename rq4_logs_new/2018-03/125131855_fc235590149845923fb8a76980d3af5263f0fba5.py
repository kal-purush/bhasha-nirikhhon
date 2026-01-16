from numpy import *
from scipy import *

def main():
    T=zeros((1,6))
    with open("landmarks.txt", 'r') as lmtxt:
        lines = lmtxt.readlines()
        matched=[i for i,x in enumerate(lines) if "Refined" in x]

        
        for i in matched:
            lms1=lines[i+1].strip("\n").split("\t")
            lmsx1=lms1[0]
            lmsy1=lms1[1]
            lms2=lines[i+2].strip("\n").split("\t")
            lmsx2=lms2[0]
            lmsy2=lms2[1]
            lms3=lines[i+3].strip("\n").split("\t")
            lmsx3=lms3[0]
            lmsy3=lms3[1]
            T = vstack((T, r_[lms1,lms2,lms3]))            

        T=T[1:,:]
        savetxt("LMTable.txt",T, fmt='%s\t%s\t%s\t%s\t%s\t%s')            
        #        savetxt("LMTable.txt", T, fmt='%s')    
    
if __name__ == '__main__':
    main()