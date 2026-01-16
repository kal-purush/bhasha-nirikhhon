"3.1 Write a program to prompt the user for hours and rate per hour using input to compute gross pay. Pay the hourly rate for the hours up to 40 and 1.5 times the hourly rate for all hours worked above 40 hours. Use 45 hours and a rate of 10.50 per hour to test the program (the pay should be 498.75). You should use input to read a string and float() to convert the string to a number. Do not worry about error checking the user input - assume the user types numbers properly."


hrs = float(input("Enter Hours:"))
pph = float(input("Enter Pay Per Hours:"))

if hrs <= 40:
    ttl = hrs * pph
elif hrs > 40:
    ex = hrs - 40
    ttl = (40 * pph) + (ex * 1.5 * pph)
print(ttl)