
#This scripts will convert absorbing radiation units:

print('Absorbing radiation converter is launched!')

unit = int(input('Print your unit in Gy format: ', ))
out_format = input('Print to which format do you want to convert (mGy, mcrGy, cGy): ', )

absorb = {
        'mGy' : lambda unit: unit / 1000,
        'mcrGy' : lambda unit: unit / 1000000,
        'cGy' : lambda unit: unit / 10000,
        }

print(f'In {out_format} your radiation is:', absorb[out_format](unit))