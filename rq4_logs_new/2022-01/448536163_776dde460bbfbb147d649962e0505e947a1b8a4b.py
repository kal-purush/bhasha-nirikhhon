samochody = ["syrena", "polonez", "tesla"]

print(samochody[0])
print(samochody[1])
print(len(samochody))

print("=== FOR ===")
for s in samochody:
    print(s)


print("=== FOR ===")
print(len(samochody))
print(range(len(samochody)))

for idx in range(len(samochody)):
    s = samochody[idx]
    print("idx: {0} nazwa: {1}".format(idx, s))