import time
import numpy as np
import os
import speech_recognition as sr
from naoqi import ALProxy, ALBroker, ALModule

from sound import Transcriber

count = 0
snd = None
r = None

def connect(address, port=9559):
	global r
	r = Robot(address, port)

def robot():
	global r
	return r

class Robot():

	def __init__(self, address, port=9559):
		self.asr = ALProxy("ALSpeechRecognition", address, port)
		self.tts = ALProxy("ALTextToSpeech", address, port)
		self.mem = ALProxy("ALMemory", address, port)
		self.broker = ALBroker("broker", "0.0.0.0", 0, address, port)
		self.address = address
		self.port = port
		self.asr.setLanguage("English")

		self.yes_no_vocab = {
			"yes": ["yes", "ya", "sure", "definitely"],
			"no": ["no", "nope", "nah"]
		}

		self.object_vocab = {
		"digital_clock": ["digital clock", "blue clock", "black alarm clock"],
		"analog_clock": ["analog clock", "black clock", "black alarm clock"],
		"red_soccer_ball": ["red soccer ball", "red ball"],
		"basketball": ["basketball", "orange ball"],
		"football": ["football"],
		"yellow_book": ["yellow book"],
		"yellow_flashlight": ["yellow flashlight"],
		"blue_soccer_ball": ["blue soccer ball", "blue ball"],
		"apple": ["apple"],
		"black_mug": ["black mug"],
		"blue_book": ["blue book"],
		"blue_flashlight": ["blue flashlight"],
		"cardboard_box": ["cardboard box"],
		"pepper": ["pepper", "jalapeno"],
		"green_mug": ["green mug"],
		"polka_dot_box": ["polka dot box"],
		"scissors": ["scissors"]
		}

		self.vocab = self.yes_no_vocab
		self.asr.setVocabulary([j for i in self.vocab.values() for j in i], False)

	def say(self, text):
		"""
		Uses ALTextToSpeech to vocalize the given string
		"""

		self.tts.say(text)

	def ask(self, question):
		"""
		Has the robot ask a question and returns the answer
		"""
		global count
		print question
		count += 1
		if count == 1:
			return "no"
		elif count == 2:
			return "yes"
		elif count == 3:
			return "yes"
		elif count == 4:
			return "yes"
		elif count == 5:
			return "no"
		elif count == 6 or count == 7:
			return "yes"

		# self.say(question)
		# self.asr.subscribe("TEST_ASR")
		# data = (None, 0)
		# while not data[0]:
		# 	data = self.mem.getData("WordRecognized")
		# self.asr.unsubscribe("TEST_ASR")
		#
		# print data
		#
		# for word in self.vocab:
		# 	for syn in self.vocab[word]:
		# 		if data[0] == syn:
		# 			return word

	def ask_object(self):
		print("What object were you thinking of?")

		transcriber = Transcriber("transcriber", self.address, self.port)
		transcriber.start()

		while True:
			if transcriber.check:
				break
		transcriber.stop()
		os.system("sox -r 48000 -e signed -b 16 -c output.raw speech.wav")

		r = sr.Recognizer()
		with sr.WavFile("speech.wav") as source:
			speech = r.record(source)
		try:
			possibilities = r.recognize(speech,True)
			for possibility in possibilities:
				for word in self.object_vocab:
					for syn in self.object_vocab[word]:
						if possibility["text"] == syn:
							print possibility
							self.broker.shutdown()
							return possibility
		except LookupError:
			self.say("I couldn't understand what you said. Please go to the computer and type the name of your object.")
			self.broker.shutdown()
			return raw_input("What object were you thinking of?")