from python_speech_features import mfcc 
from pydub.utils import make_chunks
from pydub import AudioSegment
import scipy.io.wavfile as wav
import numpy as np
import os


'''=============== EXTRACT FEATURE ==============='''

def extractFeatures():
	en_data = {"input_list": ["./input/en19.wav", "./input/en23a.wav", "./input/en23b.wav", "./input/en32.wav"], "output": "./feature_output/en.txt"}
	es_data = {"input_list": ["./input/es1.wav", "./input/es2.wav", "./input/es3.wav"], "output": "./feature_output/es.txt"}
	pl_data = {"input_list": ["./input/pl3.wav", "./input/pl8.wav", "./input/pl26.wav"], "output": "./feature_output/pl.txt"}
	data = [en_data, es_data, pl_data]
	for d in data:
		for i in d["input_list"]:
			calculateMFCC(i, d["output"])

def calculateMFCC(inputfile, outputfile):
	'''
	use the python_speech_features library to comute the MFCC for each sample
	save the sames to a txt file
	'''
	audio_data = AudioSegment.from_file(inputfile, "wav")
 	chunk_length_ms = 6 * 1000	# 6 second clips
 	chunks = make_chunks(audio_data, chunk_length_ms) #chunks not necessary anymore 
	try:
		os.remove(outputfile)
	except OSError:
		pass
	tempfilename = "./feature_output/temp.wav"
 	output_file_obj = open(outputfile, "a")
 	for chunk in chunks:
 		chunk.export(tempfilename, format="wav") 
 		(rate, data) = wav.read(tempfilename)    
 		mfcc_feat = mfcc(data, rate)
 		np.savetxt(output_file_obj, mfcc_feat)
 		output_file_obj.write("\n")
 	output_file_obj.close()
 	os.remove(tempfilename)