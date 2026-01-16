from hashlib import sha256

def log_in_code_tool():
    while True:
        print("ihranA KodE: ",sha256(input("Setzen zie HiEr ElekTroniSche MaiL Addresse ein ; )   |: ").encode('utf-8')).hexdigest())
        y=input("Hammas ? Y für ja anderes für nein")
        if y == "Y":
            break