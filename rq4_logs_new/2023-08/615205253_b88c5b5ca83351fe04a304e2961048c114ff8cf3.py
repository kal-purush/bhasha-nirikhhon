'''
 programma Python che converte un numero decimale periodico in una frazione
'''

from fractions import Fraction

def decimal_to_fraction(decimal):
    num_digits = len(decimal) - 2  # Ignora "0." iniziale
    num = int(decimal.replace(".", ""))
    denom = 10 ** num_digits
    
    gcd = Fraction(num, denom).denominator
    fraction = Fraction(num // gcd, denom // gcd)
    
    return fraction

decimal = input("Inserisci il numero decimale periodico (es. 0.0833333333333333): ")
fraction = decimal_to_fraction(decimal)

print("La frazione equivalente è:", fraction)