import otp


def main():
    key = input("Enter keyfile name: ")
    ct_name = input("Enter cipertextfile name: ")

    file = open(ct_name, "rb")
    ciphertext = file.read()
    file.close()

    plaintext = otp.decrypt(ciphertext, key)
    print("Decrypted: ", plaintext)


if __name__ == "__main__":
    main()