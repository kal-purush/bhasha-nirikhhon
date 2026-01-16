import time

input_string = ['I','P','Q','H','U','G','C','X','Z','M']
answer = ['H','E','I','L','H','I','T','L','E','R']
final_plugboard = list()
final_rotorI = list()
final_rotorII = list()
final_rotorIII = list()
final_start = list()

def validation():
    correct = False
    time_start = time.time()
    time_end = 0
    error = 0
    while(not correct):
        if answer == enigma(plugboard,rotorI,rotorII,rotorIII,start_position,input_string):
            print("correct!")
            final_plugboard = list(plugboard)
            final_rotorI = list(rotorI)
            final_rotorII = list(rotorII)
            final_rotorIII = list(rotorIII)
            final_start = list(start)
            correct = True
            time_end = time.time()
            print("[error:",error,"] [It cost %f sec to correct]" % (time_end - time_start))
        else:
            error += 1
            if error%1000000 == 0:
                time_end = time.time()
                print("[error:", error/1000000, "million]  ", "[sec:", (time_end - time_start),"]")


if __name__ == "__main__":
	validation()
