#/AdvNetwLab$ tshark -r Lab_4/traces/L4-4-2.STA1.pcap -T fields -e wlan.wep.iv > tshark2.out

from collections import defaultdict
if __name__ == "__main__":
    f = open("tshark2.out")
    ivdict=  defaultdict(int)
    lines_with_space = f.readlines()
    lines = []

    for line in lines_with_space:
        if (line[:-1]):
            lines.append(line[:-1])

    for line in lines:
        ivdict[line] += 1

    occurTwice = 0
    total = 0
    for key in ivdict.keys():
        if (ivdict[key] > 1):
            occurTwice += 1
        total += ivdict[key]

    print(total)
    print("")
    print("We have " + str(len(lines)) + " different keys. Out of those we have " + str(len(ivdict.keys())) + " unique IV's of which " + str(occurTwice) + " occured more than once.")
    f.close()