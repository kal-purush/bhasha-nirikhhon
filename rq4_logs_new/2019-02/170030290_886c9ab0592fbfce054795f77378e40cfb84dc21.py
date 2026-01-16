import hashlib
from base64 import b64decode, b64encode

GROUP_ID = "480145"
CHALLENGE_FROM_SERVER = bytearray.fromhex("2d313334393333353530370a")  # "1349335507"   "2d313334393333353530370a"  "31333439333335353037"
TARGET_HEX_CODE = "6dc96811969dcbebb63a0da7c836215f47e5ac43cd4c268190d7e0942f393615"


def main():
    with open("question2_clients/cracklib-small", 'r') as cracklib:
        cracklib_dict = cracklib.readlines()
    print("The cracklib has " + str(len(cracklib_dict)) + " items.")

    for passwd in cracklib_dict:
        original_passwd = passwd.strip('\n')
        constructed_string = GROUP_ID.encode() + ":".encode() + CHALLENGE_FROM_SERVER + ":".encode() + original_passwd.encode()
        hex_guess = hashlib.sha256(constructed_string).hexdigest()
        if hex_guess == TARGET_HEX_CODE:
            print("\nFind original password:\t" + original_passwd)
            return
    print("\nNone of password matches.")


if __name__ == '__main__':
    main()

