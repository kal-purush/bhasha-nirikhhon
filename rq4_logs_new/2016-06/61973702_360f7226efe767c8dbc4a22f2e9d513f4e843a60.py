from string import ascii_lowercase

def KSA(key):
	"""This initialises the permutation in array S."""
	keylength = len(key)
    
	# 256 is the max keylength
	S = list(range(256))

	j = 0
	for i in range(256):
		j = (j + S[i] + key[i % keylength]) % 256
		#  swap values of S[i] and S[j]
		S[i], S[j] = S[j], S[i]  # swap
    
	return S

def PRGA(S):
	"""Initialsises the pseudo-random generator, which takes in values of S"""
    
	Klist = []
	i = 0
	j = 0
	while True:
		# increments i, and looks up the ith element of S, S[i]
		i = (i + 1) % 256
		# which it then adds to j
		j = (j + S[i]) % 256
		# swaps again
		S[i], S[j] = S[j], S[i]  # swap
		# use the sum S[i] + S[j] mod 256 as an index to find a third element of S
		K = S[(S[i] + S[j]) % 256]
		# like return, but for generator functions 
		yield K


def RC4(key):
	"""Calls the main workings of the RC4 cipher."""
	S = KSA(key)
	return PRGA(S)



def convert_key(s):
	"""Converts keys into unicode format."""
	return [ord(c) for c in s]
    
    
def encrypt():
	"""This is the encryption function."""
	key = input("Input your encryption key : ")
	plaintext = input("Input your plaintext : ").replace(' ', '').lower()  # to remove input spaces
	while not plaintext.isalpha():
		print('Please only input letters!\n')
		plaintext = input("Input your plaintext : ").replace(' ', '').lower()
        
	plaintextlist = []
	for i in range(len(plaintext)):
		plaintextlist.append(ascii_lowercase.index(plaintext[i]))
    
	key = convert_key(key)
	keystream = RC4(key)

	for i in range(len(plaintextlist)):
		plaintextlist[i] = ascii_lowercase[(plaintextlist[i] + next(keystream)) % 26]
	print("This is your ciphertext : " + ''.join(plaintextlist))

def decrypt():
	"""This is the decryption function."""
	key = input("Input your encryption key : ")
	ciphertext = input("Input your ciphertext : ")
        
	cipherlist = []
	for i in range(len(ciphertext)):
		cipherlist.append(ascii_lowercase.index(ciphertext[i]))
    
	key = convert_key(key)
	keystream = RC4(key)

	for i in range(len(cipherlist)):
		cipherlist[i] = ascii_lowercase[(cipherlist[i] - next(keystream)) % 26]
	print("This is your plaintext : " + ''.join(cipherlist))

# this is the main screen (now with ASCII fun!)
title = """
 ██████╗  ██████╗██╗  ██╗
██╔══██╗██╔════╝ ██║  ██║
██████╔╝██║      ███████║
██╔══██╗██║      ╚════██║
██║  ██║╚██████╗      ██║
╚═╝  ╚═╝ ╚═════╝      ╚═╝
                        
       in Python                   
                       """
print(title)

# This is the main boilerplate of the program, where the functions are called.
while True:	
	print("=== Main Menu ===")
	print('1. Encrypt\n2. Decrypt')
	choice = input('What would you like to do? : ')
	print('')
	if choice == '1': # encryption
		encrypt()
		print('')
		continue
	elif choice == '2': # decryption
		decrypt()
		print('')
		continue
	else:
		print('Invalid choice! Enter either 1 or 2.')
		print('')
		continue