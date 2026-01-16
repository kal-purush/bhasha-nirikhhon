encoded_string = "12oocvz09jqquiH11yywxsanmvbpI13yer6twweop14s 21gebwvsrxeqdwygrtuhijwT11hhllknbvxzdH00E01iR10kkjsagetrdE"

# empty lists to store found characters
DECODED_WORD = []
DECODED_WORD_MULTIDIGIT = []

# converts the encoded string into a list of individual characters
CHARACTER_LIST = list(encoded_string)

LENGTH = len(encoded_string)


def main(encoded_string):

	i = 0

	if CHARACTER_LIST[i].isdigit() == True:

		plaintext(encoded_string)


	if CHARACTER_LIST[i + 1].isdigit() == True:

		plaintext_multidigit(encoded_string)