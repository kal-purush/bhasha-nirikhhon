# Feet To Inches
# 7/1/17
# CTI-110 M5T2_FeetToInches
# Patricia Hicks
#
INCHES_PER_FOOT = 12

def main():
    feet = int(input('Enter a number of feet: '))

    print(feet, 'equals', feet_to_inches(feet), 'inches. ')

def feet_to_inches(feet):
    return feet * INCHES_PER_FOOT

main()
