import sys  # Import sys to accept the file at the command line

print("Steven Jermstad, BNFO301 fastqstats")

try:    # try to accept file
    fastqName = sys.argv[1]
except:      # Tell user if command line is empty
    print("Command line empty ")
    quit()
 
fastqFile = open(fastqName, 'r')  # Opens the file
# Lists to store the sequences and Quality Score data
sequences = []
qScore = []
x = 1
for line in fastqFile:
    line = line.strip()
    if x == 2:           # Stores every second line to the sequences list
        sequences.append(line)  # removes the \n at the end of the string
    if x == 4:           # Stores every forth line to the qScore list
        qScore.append(line)  # removes the \n at the end of the string
        x = 0
    x += 1

bases = 0
seqList = []
GCContent = 0
for seq in sequences:     # Loops through the sequence list to find base count, read count and GC count
    bases += len(seq)
    seqList.append(len(seq))
    GCContent += seq.count("G")
    GCContent += seq.count("C")
reads = len(sequences)
GCPercent = (float(GCContent) / float(bases)) * 100


readQuality = 0
for scores in qScore:     # Loops through the list of quality scores
	for char in scores:
		asciiScore = (ord(char) -33)          # Converts each character in the qSqore list to unicode number and subtracts 33
		if asciiScore >= 0 and asciiScore <= 40:     # Makes sure the values are within appropriate range
			readQuality += asciiScore
aveReadQuality = readQuality / bases
# Prints the answers to console
print("Total number of bases: " + str(bases))
print("Read Number: " + str(reads))
print("GC base count: " + str(GCContent))
print("GC content: " + str(round(GCPercent, 2)) + "%")
print("Average read quality: " + str(aveReadQuality))
print("Average Read Length: " + str(bases/reads))
print("Maximum Read length: " + str(max(seqList)))
print("Minimum Read length: " + str(min(seqList)))