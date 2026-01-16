from temperature import *
import time
import max30102
import hrcalc

def temp_chk():
    print(' rom: '+ read_rom())
    tmp = read_temp() #Read temperature
    print(f"Your current temperature is {tmp[0]}C {tmp[1]}F")
    return tmp[0]

def Oksi(devStatus):
    m = max30102.MAX30102()

    hr2 = 0
    sp2 = 0
    red, ir = m.read_sequential()
    
    hr,hrb,sp,spb = hrcalc.calc_hr_and_spo2(ir, red)

    if devStatus:

        print("hr detected:",hrb)
        print("sp detected:",spb)
        
        if(hrb == True and hr != -999):
            hr2 = int(hr)
            print("Heart Rate : ",hr2)
        if(spb == True and sp != -999):
            sp2 = int(sp)
            print("SPO2       : ",sp2)

    return hr2, sp2

def mainCheck():
    Oksi(False)
    print('Mengukur, tunggu 2 menit...')
    time.sleep(60)
    print('1 menit lagi...')
    time.sleep(30)
    print('30 detik lagi...')
    time.sleep(30)
    temp_chk()

    rslt = Oksi(True)
    print(f'Hearth Rate: {rslt[0]}, SPO2: {rslt[1]}')

if __name__ == '__main__':
    mainCheck()
