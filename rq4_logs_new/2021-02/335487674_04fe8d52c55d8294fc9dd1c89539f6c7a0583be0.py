
def fitness(short, long, binary):
    candidate = ""
    for x in range(len(binary)):
        if binary[x] == "1":
            candidate += (short[x])
    print("Candidate: ", candidate)
    place = 0
    match = 0
    for y in range(len(candidate)):
        for z in range(len(long)):
            if z == 0:
                z = place
            print("place", place, candidate[y],":::" ,long[z])
            if candidate[y] == long[z]:
                print("y = ", y, "z = ", z)
                place = z + 1
                match += 1
                print("Place: ", place, "z", z)
                break

    print("Long: ", long)
    print("Match: ", match)


def main():
  print("Sup my nug")
  fitness("popo", "poooooooooopoo", "1110")
  #fitness("president", "providence", "110011110") #priden
  #fitness("president", "providence", "100000101") #pet

if __name__ == '__main__':
  main()