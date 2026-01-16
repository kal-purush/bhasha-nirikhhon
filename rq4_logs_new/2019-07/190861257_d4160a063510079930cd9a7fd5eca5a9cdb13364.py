from queue import Queue
import os
import re
import sys
import threading

class ReadConcurrently:
	
	def __init__(self, _path="/", nThread=8):
		self.queueRetrieval = Queue()
		self.queueRead = Queue()
		self.NUMBER_OF_THREADS = nThread
		self.FOLDER_AND_FILE_PATH = _path
		self.FILE = set()
		self.TXT_FILES = dict()
		self.FILE_EXTENSIONS = ('txt', ) # 'pdf', 'doc', 'ppt') # TODO
		self.FILE_FLAG = [False]
		self.FOLDER = set()
		self.OTHER = set()
		self.read_files(self.main(self.FOLDER_AND_FILE_PATH))
	
	def read_file(self, file_path):
		try:
			print(threading.current_thread().name, " adding from ", file_path)
			if not file_path[file_path.rfind('.') + 1:] in self.FILE_EXTENSIONS: return
			with open(file_path) as file:
				self.TXT_FILES[file_path] = sorted(set(re.sub("[^a-zA-Z0-9]", " ", file.read().upper()).split()))
				file.close()
			return None
		except IsADirectoryError as Msg: pass
	
	def parse_dir(self, path):
		try:
			for _ in os.listdir(path):
				if os.path.isdir(path + _):
					self.FOLDER.add(path + _ + '/')
					self.parse_dir(path + _ + '/')
				elif os.path.isfile(path + _): self.FILE.add(path + _)
				else: self.OTHER.add(path + _)
		except PermissionError as Msg: print(str(Msg))
		except FileNotFoundError as Msg: print(str(Msg))
		except NotADirectoryError as Msg: print(str(Msg))
		return None
	
	def create_workers(self):
		for _ in range(self.NUMBER_OF_THREADS):
			t = threading.Thread(target=self.work)
			t.daemon = True
			t.start()
	
	def work(self):
		if self.FILE_FLAG[0] == False:
			while True:
				path = self.queueRetrieval.get()
				print(threading.current_thread().name, " parsing ", path)
				self.parse_dir(path)
				self.queueRetrieval.task_done()
			return None
		elif self.FILE_FLAG[0] == True:
			while True:
				full_file_path = self.queueRead.get()
				print(threading.current_thread().name, " reading ", full_file_path)
				try: self.read_file(full_file_path)
				except UnicodeDecodeError as Msg: print(str(Msg) + full_file_path)
				self.queueRead.task_done()
			return None
		return None
	
	def create_jobs(self, directory):
		if type(directory) == set:
			for p in directory: self.queueRead.put(p)
			self.queueRead.join()
			return 'job created from directory set'
		if directory == '/' or directory == '\\': pass
		elif directory[-1] != '/' and directory[-1] != '\\': directory += '/'
		try:
			for files_dir in os.listdir(path=directory):
				if os.path.isdir(directory + files_dir):
					self.queueRetrieval.put(directory + files_dir + '/')
					self.FOLDER.add(directory + files_dir + '/')
				elif os.path.isfile(directory + files_dir): self.FILE.add(directory + files_dir)
			self.queueRetrieval.join()
			return 'job created from the directory'
		except FileNotFoundError as Msg: print(str(Msg))
		del directory
	
	def main(self, dy_path):
		self.create_workers()
		print(self.create_jobs(dy_path))
		return self.FILE
	
	# delete everything at the end
	def destruct():
		pass
	
	@staticmethod
	def print_dir(lst):
		if type(lst) == dict:
			for (key, value) in lst.items():
				print(key)
				for val in value:
					print('\t\t\t', val)
					return
		for item in lst: print(item)
	
	# read and load every txt file
	def read_files(self, File):
		self.FILE_FLAG[0] = True
		self.main(File)
	
	def feed(self): return self.TXT_FILES


def write_dir(item):
	with open('/sdcard/eden.txt', 'a') as writer:
		writer.write(item + '\n')
		writer.close()
