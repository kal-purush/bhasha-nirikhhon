import wave
import random

def wavSramble(filename, outputName, frameSize = 1000, verbose = False,):
	'''Takes wav file and scrambles based on framesize'''
	file = wave.open(filename,'rb')
	newfile = wave.open(outputName, 'wb')

	params = file.getparams()
	newfile.setparams(params)

	li = []
	for i in range(params[3] // frameSize):
		file.setpos(i * frameSize)
		li.append(file.readframes(frameSize))

	random.shuffle(li)

	by = bytearray()
	for i in range(len(li)):
		if verbose == True:
			print(i, ' of ', len(li) - 1)
		by = bytearray(li[i])
		newfile.writeframes(by)

	if verbose == True:
		print('Done.')
		input()

if __name__ == '__main__':
	filename = input('File: ')
	outputName  = input('New file name: ')
	frameSize = int(input('Framesize: '))
	wavSramble(filename, outputName, frameSize, verbose = True)