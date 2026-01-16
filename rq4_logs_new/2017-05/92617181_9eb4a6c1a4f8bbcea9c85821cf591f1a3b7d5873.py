import pickle

class Message:

	def __init__(self, data):
		self.data = data
		self.data_length  = len(data)

	def serialize(self):
		pass

	@staticmethod
	def deserialize(data):
		return pickle.loads(data)

	@staticmethod
	def serialize(data):
		return pickle.dumps(self)
		
class TransactionRequest(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, T):
		self.transaction = T
		super(RequestVoteResponse, self).__init__(self.serialize())

class TransactionResponse(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, T):
		self.transaction = T
		super(RequestVoteResponse, self).__init__(self.serialize())