import math
import random


def modularExponent(x, a, n):
    binary = bin(a)[2:]
    s = 1
    k = 0
    r = 0
    while (k < len(binary)):
        r = (s * x) % n if binary[k] == "1" else s
        s = (r ** 2) % n
        k += 1
    return r


def isPrime(n):
    highestPower = ((n-1) & (~(n - 2)))
    m = (n - 1) // highestPower
    k = int(math.log2(highestPower))
    a = random.randint(1, n)
    if (m % 2) == 0:
        return False
    else:
        b = modularExponent(a, m, n)
        if b == 1 or b == n - 1:
            return True
        else:
            for i in range(1, k - 1):
                b = modularExponent(b, 2, n)
                if b == (1 % n):
                    return False
                else:
                    if b == (-1 % n):
                        return True
            return False


def randomLargeOdd():
    randInt = random.getrandbits(512)
    if randInt % 2 == 0:
        randInt += 1
    return randInt


def extendedEuclid(a, b):
    if a == 0:
        return b, 0, 1
    gcd, xj_1, yj_1 = extendedEuclid(b % a, a)
    x = yj_1 - (b//a) * xj_1
    y = xj_1
    return gcd, x, y


def generateLargePrime():
    randInt = randomLargeOdd()
    while(isPrime(randInt) != True):
        randInt = randomLargeOdd()
    return randInt


def createKeys():
    e = 65537
    p = generateLargePrime()
    q = generateLargePrime()
    gcd, x, y = extendedEuclid(e, (p - 1)*(q - 1))
    while(p - q < 10**95 or gcd != 1):
        p = generateLargePrime()
        q = generateLargePrime()
        gcd, x, y = extendedEuclid(e, (p - 1)*(q - 1))
    n = p * q
    d = x
    return n, e, d


def encryptMessage(m, n, e):
    return modularExponent(m, e, n)


def decryptMessage(c, n, d):
    return modularExponent(c, d, n)


n, e, d = createKeys()

public = open('public_key', 'w')
private = open("private_key", 'w')

public.write("%i\n%i" % (n, e))
public.close()

private.write(str(d))
private.close()

message = open('message', 'r')
m = int(message.readline()[:-1])
message.close()

c = encryptMessage(m, n, e)
cipher = open('ciphertext', 'w')
cipher.write(str(c))
cipher.close()

p = decryptMessage(c, n, d)
decrypt = open('decrypted_message', 'w')
decrypt.write(str(p))
decrypt.close()