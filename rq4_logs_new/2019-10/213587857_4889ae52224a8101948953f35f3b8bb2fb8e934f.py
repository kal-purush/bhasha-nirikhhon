def encrypt_caesar(plaintext):
    ciphertext = ""
    for i in range (0, len(plaintext)):
        if ord("A") <= ord(plaintext[i]) <= ord("Z"):
            ciphertext += chr((ord(plaintext[i]) + 3 - ord("A")) % 26 + ord("A"))
        elif ord("a") <= ord(plaintext[i]) <= ord("z"):
            ciphertext += chr((ord(plaintext[i]) + 3 - ord("a")) % 26 + ord("a"))
        else:
            ciphertext += plaintext[i]
    return ciphertext

def decrypt_caesar(ciphertext):
    plaintext = ""
    for i in range (0, len(ciphertext)):
        if ord("A") <= ord(ciphertext[i]) <= ord("Z"):
            plaintext += chr((ord(ciphertext[i]) - 3 - ord("A")) % 26 + ord("A"))
        elif ord("a") <= ord(ciphertext[i]) <= ord("z"):
            plaintext += chr((ord(ciphertext[i]) - 3 - ord("a")) % 26 + ord("a"))
        else:
            plaintext += ciphertext[i]
    return plaintext