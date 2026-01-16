'''
Author: Devin Whitten
# This function essentially cleans up the input data.

1) Rejects the elements above specified SIGMA
2) Rejects keyword flags, such as SATURATION

'''

import numpy as np
import matplotlib.pyplot as plt
import Regression as reg
from os import environ

fail = 0
''' SPECIFICATIONS '''
#------------------------------------------------------------------------------
Display_Fields = 'n' ## raw_input("Display Fields?:  (y/n)  ")
#Scale = float(raw_input("Scale for SD rejection?:   "))
SDSS_SAT = 16.0 #float(raw_input("SDSS Saturation Limit?: (Shelf ~ 15, MIN ~ 10)  "))
SDSS_DIM = 22.0 #float(raw_input("SDSS Dim Cutoff? (~25):   "))
GALEX_SAT = 10.0 #float(raw_input("GALEX Saturation Limit? (14 - 10):   "))
GALEX_DIM = 23.0 #float(raw_input("GALAX Dim Cutoff? (24):   "))

#------------------------------------------------------------------------------

print "CHANGE"
while(fail == 0):
    #SheetName = raw_input("Please input name of column data:  ")
    home = environ["HOME"]
    SheetName = home + '/Documents/Survey Astronomy/Refine_Catalogue/Clean_Catalogue_2arc.csv'
    print SheetName

    try:
        fh = open(SheetName)
        fail = 1
    except:
        print "Invalid file name"
        fail = 0


Line = fh.readline().split(',')

if(Display_Fields == 'y'):
    for i in range(len(Line)):
        print i, Line[i]


### TEST DELETE  ########
count = 0
FU = []
FU_err = []

NU = []
NU_err = []

U_SDSS = []
U_SDSS_err = []

G_SDSS = [] #Field 23
G_SDSS_err = [] #Field 28

R_SDSS = [] #Field 24
R_SDSS_err = [] #Field 29

I_SDSS = [] #Field 25
I_SDSS_err = [] #Field 30

Z_SDSS = [] #Field 26
Z_SDSS_err = [] #Field 31

Flag = [] #Field 35
E_bv = [] # Field 8

maxstars = 90000 # This is more than the number of stars in the cat

#------------------------------------------------------------------------------
''' LOAD RAW ARRAYS FROM CATALOG'''
Field_Numbers = [22, 27, 23, 28, 23, 28, 24, 29, 3, 5, 25, 30, 27, 31, 8]
for line in fh:

    ''' ENSURE NO EMPTY ELEMENTS'''
    #----------------------------------------
    flag = 'FALSE'
    if (count > 0):
        Newline = line.split(',')
        #print Newline[3], Newline[22]
        for element in Field_Numbers:
            if(len(Newline[element]) == 0):
                flag = 'TRUE'
    #----------------------------------------
        if (flag == 'FALSE'):       # PROCEEDS IF NO FLAG RAISED FOR EMPTY ELEMENTS

            ''' SDSS LOADING '''
            try:
                # SDSS ARRAYS
                U_SDSS.append(float(Newline[22]))
                U_SDSS_err.append(float(Newline[27]))
                Flag.append(Newline[35])
                E_bv.append(float(Newline[8]))

            except:
                print 'ERROR: U_SDSS/Flag/E_bv GRAB ELEMENT FAIL'

            try:
                G_SDSS.append(float(Newline[23]))
                G_SDSS_err.append(float(Newline[28]))

            except:
                print "ERROR: G_SDSS GRAB ELEMENT FAIL"
                print "G_SDSS ELEMENT:  ", Newline[23]
                print 'G_SDSS_err ELEMENT:  ', Newline[28]

            try:
                R_SDSS.append(float(Newline[24]))
                R_SDSS_err.append(float(Newline[29]))

            except:
                print "ERROR: R_SDSS GRAB ELEMENT FAIL"
                print 'R_SDSS:  ', Newline[24]
                print 'R_SDSS_err:  ', Newline[29]

            try:
                I_SDSS.append(float(Newline[25]))
                I_SDSS_err.append(float(Newline[30]))

            except:
                print "ERROR: I_SDSS GRAB ELEMENT FAIL"
                print "I_SDSS:   ", Newline[25]
                print "I_SDSS_err:   ", Newline[30]

            try:
                Z_SDSS.append(float(Newline[26]))
                Z_SDSS_err.append(float(Newline[31]))

            except:
                print "ERROR: Z_SDSS GRAB ELEMENT FAIL"
                print "Z_SDSS:   ", Newline[26]
                print "Z_SDSS_err    ", Newline[31]

            ''' GALEX ARRAYS '''
            try:
                # GALEX ARRAYS
                FU.append(float(Newline[3]))
                FU_err.append(float(Newline[5]))
            except:
                print 'ERROR: FU_GALEX ELEMENT GRAB FAIL'
                print 'FU_GALEX ELEMENT:  ', Newline[3]
                print 'FU_GALEX_err ELEMENT:   ', Newline[5]

            try:
                NU.append(float(Newline[4]))
                NU_err.append(float(Newline[6]))
            except:
                print "ERROR: NU_GALEX GRAB ELEMENT FAIL"
                print 'NU_GALEX ELEMENT:  ', Newline[4]
                print 'NU_GALEX_err ELEMENT:   ', Newline[6]

    count += 1
    if count > maxstars:
        break;

Stars = len(U_SDSS)
print "-----------------------------------------------------------"
print "Number of Stars: ", len(U_SDSS)

'''
print "NU,    NU_err,     FU,     FU_err"
for i in range(100):
    print NU, NU_err, FU, FU_err
'''

## Let's compute average and standard deviations of the Magnitude
FU_err_AVERAGE = float(np.average(FU_err))
FU_err_SD = float(np.std(FU_err))
NU_err_AVERAGE = float(np.average(NU_err))
NU_err_SD = float(np.std(NU_err))

U_SDSS_AVG_ERR = float(np.average(U_SDSS_err))
U_SDSS_SD_ERR = float(np.std(U_SDSS_err))

G_SDSS_AVG_ERR = float(np.average(G_SDSS_err))
G_SDSS_SD_ERR = float(np.std(G_SDSS_err))

R_SDSS_AVG_ERR = float(np.average(R_SDSS_err))
R_SDSS_SD_ERR = float(np.std(R_SDSS_err))

I_SDSS_AVG_ERR = float(np.average(I_SDSS_err))
I_SDSS_SD_ERR = float(np.std(I_SDSS_err))

Z_SDSS_AVG_ERR = float(np.average(Z_SDSS_err))
Z_SDSS_SD_ERR = float(np.std(Z_SDSS_err))


'''
# CALCULATE AND DISPLAY AVERAGE ERROR AND STANDARD Deviation
'''
print
print "GALEX FILTER STATISTICS"
print "-----------------------------------------------------------"
print "Far UBand Average Error:  ", FU_err_AVERAGE
print "Far UBand Standard Deviation:  ", FU_err_SD
print "Near UBand Average Error:  ", NU_err_AVERAGE
print "Near UBand Standard Deviation:  ", NU_err_SD
print

print "SDSS FILTER STATISTICS"
print "-----------------------------------------------------------"
print "UBand Average Error:  ", U_SDSS_AVG_ERR
print "UBand Error Sigma:  ", U_SDSS_SD_ERR
print "GBand Average Error:  ", G_SDSS_AVG_ERR
print "GBand Error Sigma:  ", G_SDSS_SD_ERR
print "RBand Average Error:  ", R_SDSS_AVG_ERR
print "RBand Error Sigma:  ", R_SDSS_SD_ERR
print "IBand Average Error:  ", I_SDSS_AVG_ERR
print "IBand Error Sigma:   ", I_SDSS_SD_ERR
print "ZBand Average Error:  ", Z_SDSS_AVG_ERR
print "ZBand Error Sigma:  ", Z_SDSS_SD_ERR
print ""

print "B-V Color Statistics"
print "Extinction Average:   ", np.average(E_bv)
print "Extinction Standard Deviation:   ", np.std(E_bv)
print "-----------------------------------------------------------"

''' INITIALIZE ARRAYS FOR REFINED SELECTION '''
FU_REF = []
FU_REF_err = []
NU_REF = []
NU_REF_err = []

U_SDSS_REF = []
U_SDSS_REF_err = []
Flag_Ref = []
G_SDSS_REF = []
G_SDSS_REF_err = []

R_SDSS_REF = []
R_SDSS_REF_err = []

I_SDSS_REF = []
I_SDSS_REF_err = []

Z_SDSS_REF = []
Z_SDSS_REF_err = []

E_bv_REF = []

##FU = []
##FU_err = []
##NU = []
##NU_err = []
##U_SDSS = []
##U_SDSS_err = []
Scale = 0.05 # DETERMINES HOW MUCH OF SIGMA TO SUBTRACT FROM AVERAGE

print type(FU_err[2]), type(FU_err_AVERAGE), type(FU_err_SD)

FU_1D = FU_err_AVERAGE - Scale * FU_err_SD
NU_1D = NU_err_AVERAGE - Scale * NU_err_SD
U_SDSS_1D = U_SDSS_AVG_ERR - Scale * U_SDSS_SD_ERR
G_SDSS_1D = G_SDSS_AVG_ERR - Scale * G_SDSS_SD_ERR
R_SDSS_1D = R_SDSS_AVG_ERR - Scale * R_SDSS_SD_ERR
I_SDSS_1D = I_SDSS_AVG_ERR - Scale * I_SDSS_SD_ERR
Z_SDSS_1D = Z_SDSS_AVG_ERR - Scale * Z_SDSS_SD_ERR

print len(FU_err), len(NU_err), len(U_SDSS_err)
for i in range(len(U_SDSS)):
    if ( (FU_err[i] < FU_1D) & (NU_err[i] < NU_1D) & (U_SDSS_err[i] < U_SDSS_1D) & (G_SDSS_err[i] < G_SDSS_1D) & (R_SDSS_err[i] < R_SDSS_1D)):
        ''' These are the refinded arrays for the U magnitudes '''

        FU_REF.append( FU[i] )
        FU_REF_err.append( FU_err[i] )

        NU_REF.append( NU[i] )
        NU_REF_err.append( NU_err[i] )

        U_SDSS_REF.append( U_SDSS[i] )
        U_SDSS_REF_err.append( U_SDSS_err[i] )
        Flag_Ref.append(Flag[i])

        G_SDSS_REF.append( G_SDSS[i])
        G_SDSS_REF_err.append( G_SDSS_err[i] )

        R_SDSS_REF.append( R_SDSS[i] )
        R_SDSS_REF_err.append( R_SDSS_err[i])

        I_SDSS_REF.append( I_SDSS[i] )
        I_SDSS_REF_err.append( I_SDSS_err[i] )

        Z_SDSS_REF.append( Z_SDSS[i] )
        Z_SDSS_REF_err.append( Z_SDSS_err[i] )

        E_bv_REF.append(E_bv[i])


print "FU_REF,    FU_REF_err,    NU_REF,    NU_REF_err,     U_SDSS_REF,    U_SDSS_REF_err,     G_SDSS_REF,     G_SDSS_REF_err,     R_SDSS_REF,     R_SDSS_REF_err"

print len(FU_REF), len(FU_REF_err), len(NU_REF), len(NU_REF_err), len(U_SDSS_REF), len(U_SDSS_REF_err), len(G_SDSS_REF), len(G_SDSS_REF_err), len(R_SDSS_REF), len(R_SDSS_REF_err)
Star_Ref = len (FU_REF)


print 'Refined Number of Stars', Star_Ref
print 'Number of Rejections:   ', Stars - Star_Ref

####-----------------------------------
''' NOW WE REJECT KEYWORD FLAGS, PARTICULARLY SATURATION '''

FU_Flag_Rej = []
NU_Flag_Rej = []

''' **********  HARD LIMIT SATURATION REJECTION **********'''

FU_REJ = []
FU_REJ_err = []
NU_REJ = []
NU_REJ_err = []
U_SDSS_REJ = []

U_SDSS_REJ_err = []
Flag_Rej = []
G_SDSS_REJ = []
G_SDSS_REJ_err = []
R_SDSS_REJ = []
R_SDSS_REJ_err = []
I_SDSS_REJ = []
I_SDSS_REJ_err = []
Z_SDSS_REJ = []
Z_SDSS_REJ_err = []

E_bv_REJ = []


for i in range(Star_Ref):
    SDSS_Bright = min(U_SDSS_REF[i], G_SDSS_REF[i], R_SDSS_REF[i])
    SDSS_Dim  = max(U_SDSS_REF[i], G_SDSS_REF[i], R_SDSS_REF[i])
    if ( ( NU_REF[i] > GALEX_SAT) & (NU_REF[i] < GALEX_DIM) & (SDSS_Bright > SDSS_SAT) & (SDSS_Dim < SDSS_DIM)):

        FU_REJ.append( FU_REF[i] )
        FU_REJ_err.append( FU_REF_err[i] )

        NU_REJ.append( NU_REF[i] )
        NU_REJ_err.append( NU_REF_err[i] )

        U_SDSS_REJ.append( U_SDSS_REF[i] )
        U_SDSS_REJ_err.append( U_SDSS_REF_err[i] )
        Flag_Rej.append( Flag_Ref[i] )

        G_SDSS_REJ.append( G_SDSS_REF[i] )
        G_SDSS_REJ_err.append( G_SDSS_REF_err[i])

        R_SDSS_REJ.append( R_SDSS_REF[i] )
        R_SDSS_REJ_err.append( R_SDSS_REF_err[i])

        I_SDSS_REJ.append( I_SDSS_REF[i] )
        I_SDSS_REJ_err.append ( I_SDSS_REF_err[i] )

        Z_SDSS_REJ.append( Z_SDSS_REF[i] )
        Z_SDSS_REJ_err.append( Z_SDSS_REF_err[i] )

        E_bv_REJ.append(E_bv_REF[i])



'''-------------------------------------------------------------------------- '''
Star_3Rej = len(FU_REJ)

print 'Stars Remaining after Limit Rejection', Star_3Rej
print 'Number of Rejections:   ', Star_Ref - Star_3Rej

'''  ***** ****** FIELD REJECTION ******* *****'''

FU_3REJ = []
FU_3REJ_err = []

NU_3REJ = []
NU_3REJ_err = []

U_SDSS_3REJ = []
U_SDSS_3REJ_err = []

Flag_3Rej = []

G_SDSS_3REJ = []
G_SDSS_3REJ_err = []

R_SDSS_3REJ = []
R_SDSS_3REJ_err = []

I_SDSS_3REJ = []
I_SDSS_3REJ_err = []

Z_SDSS_3REJ = []
Z_SDSS_3REJ_err = []

flag_number = 0
for i in range(len(U_SDSS)):
    if( Flag[i][-5].isalpha() ):
        flag_number = flag_number
    elif( int(Flag[i][-5]) == 4 ):
        flag_number += 1

print "FLAGS:  ", flag_number
print "Number of Saturated stars in final batch:  ", flag_number
print "     ---------------WRITING---------------   "
file_out = open("Pristine_2arc_Update.csv", 'w')
file_out.write("NU,   NU_err,   FU,   FU_err,   U_SDSS,   U_SDSS_err,   G_SDSS,   G_SDSS_err,   R_SDSS,   R_SDSS_err,   I_SDSS,   I_SDSS_err,   Z_SDSS,   Z_SDSS_err,     E_bv" + "\n")
print "TEST:   ", len(U_SDSS_REJ)
for i in range(len(U_SDSS_REJ)):
    file_out.write( str(NU_REJ[i]) + "," + str(NU_REJ_err[i]) + "," + str(FU_REJ[i])
    + "," + str(FU_REJ_err[i]) + "," + str(U_SDSS_REJ[i]) + "," + str(U_SDSS_REJ_err[i])
    + "," + str(G_SDSS_REJ[i]) + "," + str(G_SDSS_REJ_err[i])
    + "," + str(R_SDSS_REJ[i]) + "," + str(R_SDSS_REJ_err[i])
    + "," + str(I_SDSS_REJ[i]) + "," + str(I_SDSS_REJ_err[i])
    + "," + str(Z_SDSS_REJ[i]) + "," + str(Z_SDSS_REJ_err[i]) + "," + str(E_bv_REJ[i]) + "\n" )