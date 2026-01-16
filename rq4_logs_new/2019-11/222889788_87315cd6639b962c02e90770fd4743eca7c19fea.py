#dna dictionary
slc = {"Isoleucine": ("ATT", "ATC", "ATA"),
       "Leucine": ("CTT", "CTC", "CTA", "CTG", "TTA", "TTG"),
       "Valine": ("GTT", "GTC", "GTA", "GTG"),
       "Phenylalanine": ("TTT", "TTC"),
       "Methionine": "ATG"
       }

#request user input
dna_user_input = input("Please enter the DNA: ")

#the function takes user input as parameter
def translate(dna_user_input):
    #declarations
    range = 0
    limitedNumber = 3

    #empty string
    new_dna = ''

    #the length of the input must be less that the limitedNumber assignend and we can increment it by 1
    while (limitedNumber < len(dna_user_input) + 1):
        #user input must range between 0 and 3
        i = dna_user_input[range:limitedNumber]
        #its the keys in slc dictionary
        for key in slc.keys():
            # its the i(dna_user_input[range:limitedNumber]) in the slc key
            if i in slc[key]:
                #will be incremented by the correct key
                new_dna += key
        #
        limitedNumber += 3
        range += 3
    #output
    print(new_dna)
#output
print (translate(dna_user_input))