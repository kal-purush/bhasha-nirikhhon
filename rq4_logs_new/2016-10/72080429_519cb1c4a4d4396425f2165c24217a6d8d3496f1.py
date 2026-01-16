x = int(raw_input("Enter the no. whose factorial is being taken : "))
print
y = int(raw_input("Enter the prime no. : "))
print
n = 1
exp = 0
while x/(y**n) > 0:
    exp += x/(y**n)
    n += 1
print exp