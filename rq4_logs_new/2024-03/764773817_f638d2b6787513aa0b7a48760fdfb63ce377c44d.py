import numpy as np, json, string

class General_Solver():
	def __init__(self, dict_path, game, num_game_boards, debug = False, end_with_close = False):


		self.game = game

		self.num_game_boards = num_game_boards

		self.debug = debug
		self.encoded_guesses = [[] for i in range(num_game_boards)]

		# Alphabet Encoding Dictionary
		self.letter_dict = dict((a, i) for i, a in enumerate(string.ascii_lowercase))

		# Load Vocabulary
		self.vocab, self.pos_vocab = self.get_initial_vocab(dict_path)
		self.local_guesses = []

		# For Optimized Performance
		self.ultra_instinct = True
		self.end_with_close = end_with_close

	def execute_final_guesses(self, game_boards):
		"""
		Recursive function.
		Finds the highest probability final guess among available game boards,
		guesses it, updates encoded guesses, and then repeats until
		there are no more available game boards.

		Input: list of numbers representing the index of game boards
		"""

		states = [self.get_state(self.encoded_guesses[i]) for i in game_boards]
		vocabs = [General_Solver.vocab_filter_total(s, self.vocab, self.pos_vocab) for s in states]
		curr = np.argmin([v[0].shape[0] for v in vocabs])
		game_boards.pop(curr)
		vocab_dist = General_Solver.get_vocab_distribution(vocabs[curr][0])
		pos_dist = General_Solver.get_vocab_distribution(vocabs[curr][1])
		info_list = General_Solver.word_information(vocabs[curr][0], vocab_dist, pos_dist, states[curr])
		new_guess = max(info_list, key = lambda x: x[1])
		max_info_guesses = sorted(info_list, key = lambda x: x[1], reverse = True)
		print([f"{General_Solver.to_word(w[0])}: {w[1]:.2f}" for w in max_info_guesses[0:10]])
		new_word_guess = General_Solver.to_word(new_guess[0])
		print(new_word_guess)
		new_enc_guesses, _, _, success = self.game.advance_state(new_word_guess)

		for i in range(self.num_game_boards):
			self.encoded_guesses[i].append(new_enc_guesses[i])

		if len(game_boards) == 0:
			return success

		self.execute_final_guesses(game_boards)

	def live_play_ultra(self):
		"""
		Trish will play Wordle Game live in ultra instinct mode,
		with optimized gameplay and automatic reading and writing.
		Variable ultra_instinct must be True to allow this mode.

		Modes:
		o - ultra_instinct
		o - debug

		"""
		final_turns = False
		done = False
		while (not final_turns) and (not done):

			if self.debug:
				inp = input("Continue")

			full_info_list = [[v, 0.0] for v in self.vocab]

			if self.game.turn == 0:
				state = self.get_state(self.encoded_guesses[0])
				vocabs = General_Solver.vocab_filter_total(state, self.vocab, self.pos_vocab)
				zero_out_states = General_Solver.get_zero_out_state(state)
				vocab_dist = General_Solver.get_vocab_distribution(vocabs[0])
				position_dist = General_Solver.get_position_distribution(vocabs[1])
				new_vocab_dist = vocab_dist * zero_out_states[0]
				new_position_dist = position_dist * zero_out_states[1]
				print(vocabs[0].shape)
				full_info_list = General_Solver.word_information(self.vocab, new_vocab_dist, new_position_dist, state)
			else:
				for i in range(self.num_game_boards):
					print(i)
					state = self.get_state(self.encoded_guesses[i])
					vocabs = General_Solver.vocab_filter_total(state, self.vocab, self.pos_vocab)
					zero_out_states = General_Solver.get_zero_out_state(state)
					vocab_dist = General_Solver.get_vocab_distribution(vocabs[0])
					position_dist = General_Solver.get_position_distribution(vocabs[1])
					new_vocab_dist = vocab_dist * zero_out_states[0]
					new_position_dist = position_dist * zero_out_states[1]
					print(vocabs[0].shape)
					info_list = General_Solver.word_information(self.vocab, new_vocab_dist, new_position_dist, state)
					for i, info in enumerate(info_list):
						full_info_list[i][1] += info[1] * vocabs[0].shape[0]
						# Scaling information to vocabulary size prioritizes game boards with more words to eliminate
			
			max_info_guesses = sorted(full_info_list, key = lambda x: x[1], reverse = True)
			print([f"{General_Solver.to_word(w[0])}: {w[1]:.2f}" for w in max_info_guesses[0:10]])
			new_guess = max(full_info_list, key = lambda x: x[1])
			new_word_guess = General_Solver.to_word(new_guess[0])
			print(new_word_guess)

			new_enc_guesses, done, final_turns, success = self.game.advance_state(new_word_guess)

			for i in range(self.num_game_boards):
				self.encoded_guesses[i].append(new_enc_guesses[i])

		if final_turns and (not done):
			success = self.execute_final_guesses([i for i in range(self.num_game_boards)])

		print("DONE")

		if self.end_with_close:
			self.game.driver.quit()

		return success

	def to_word(mat):
		"""
		Convert word matrix to string.

		Input: (5, 26) onehot encoded matrix of a 5-letter word

		Output: 5-letter word string
		"""
		return ''.join([string.ascii_lowercase[i] for i in np.argmax(mat, axis = -1).squeeze()])

	def to_onehot(encoded):
		"""
		Convert categorical words to word matrix.

		Input: 5-letter numeric words

		Output: (5, 26) onehot encoded matrix of a 5-letter word
		"""
		onehot = np.zeros([i for i in encoded.shape] + [len(string.ascii_lowercase)]).reshape(-1, len(string.ascii_lowercase))

		onehot[np.arange(0, onehot.shape[0]), encoded.reshape(-1,)] = 1.

		return onehot.reshape([i for i in encoded.shape] + [len(string.ascii_lowercase)])

	def to_position_mat(encoded):
		"""
		Convert categorical words to position matrix.

		Input: 5-letter numeric words

		Output: (26, 5) positionally encoded matrix of a 5-letter word
		"""

		new_words = np.zeros([encoded.shape[0], 26, 5])
		index_1 = np.repeat(np.arange(0, new_words.shape[0])[..., np.newaxis], 5, axis = 1).reshape(-1)
		index_2 = np.repeat(np.arange(0, 5)[np.newaxis, ...], new_words.shape[0], axis = 0).reshape(-1)

		new_words[index_1, encoded.reshape(-1), index_2] = 1.
		
		return new_words

	def get_initial_vocab(self, dict_path):
		"""
		Initializing function to get all 5-letter words and write to local variable self.vocab

		Input: path to vocabulary file (JSON)

		Outputs:
		- (5, 26) onehot encoded matrix of a 5-letter word
		- (26, 5) positionally encoded matrix of a 5-letter word
		"""
		with open(dict_path, "r") as words_read:
			words = json.load(words_read)

		encoded_words = []

		for w in words:
			encoded_words.append([self.letter_dict[l] for l in w])

		encoded_words_numpy = np.array(encoded_words)

		return General_Solver.to_onehot(encoded_words_numpy), General_Solver.to_position_mat(encoded_words_numpy)

	def get_zero_out_state(state):
		"""
		Use word-elimination state to create a binary matrix representing all possible letters and positions

		Input: (5,26) letter-position state

		Outputs: 
		- (5, 26) binary matrix of possible letters and positions
		- (26, 5) binary matrix of possible letters and positions
		"""

		zero_out = np.float32(~np.isin(state, [1., 3.]))

		return zero_out, zero_out.T
	
	def get_vocab_distribution(vocab):
		"""
		Create a probability matrix representing the vocab distribution of letters in positions

		Input: onehot-encoded vocabulary

		Output: (5, 26) probability matrix
		"""
		return vocab.mean(axis = 0)

	def get_position_distribution(pos_mat):
		"""
		Create a probability matrix representing the vocab distributions of positions of letters

		Input: position-encoded vocabulary

		Output: (26, 5) probability matrix
		"""
		return pos_mat.mean(axis = 0)

	def word_information(vocab, vocab_distribution, position_distribution, state):
		"""
		Create a list representing the information to be gained for each word

		Inputs:
		- vocabulary
		- (5, 26) letter and position probability distribution
		- (26, 5) position and letter probability distribution
		- (5,26) letter-position state

		Output: List with each item being a tuple of (onehot encoded word, information value)
		"""
		info_list = []

		for v in vocab:
			info_list.append(General_Solver.information_gained(v, vocab_distribution, position_distribution, state))
		
		return info_list


	def information_gained(w, vocab_distribution, position_distribution, state):
		"""
		Calculates the information value for a given word

		Inputs:
		- (5, 26) one hot encoded word
		- (5, 26) letter and position probability distribution
		- (26, 5) position and letter probability distribution
		- (5,26) letter-position state

		Output: Tuple containing (onehot encoded word, information value of word)
		"""
		total_info = 0.0

		repeated_letters = set()

		for i in range(w.shape[0]):
			# Probability of new information gained

			l = np.argmax(w[i])

			if state[i, l] == 3:
				continue

			if state[i, l] == 1:
				repeated_letters.add(l)
				continue
			
			if state[i, l] == 2:
				total_info +=  ((vocab_distribution[i, l] * (1. - vocab_distribution[i, l]))) * (l not in repeated_letters)
			else:
				# total_info += ((vocab_distribution[i, l] * 3.) + (vocab_distribution[np.arange(0, w.shape[0]) != i, l].sum() * 2.) + (1. - vocab_distribution[:, l].sum()) * 1.5) * (l not in repeated_letters)

				total_info +=  ((vocab_distribution[i, l] * (1. - vocab_distribution[i, l])) + (position_distribution[l, np.arange(0, 5) != i].sum() * (1. - position_distribution[l, np.arange(0, 5) != i].sum()))) + ((1. - position_distribution[l, :].sum()) * position_distribution[l, :].sum()) * (l not in repeated_letters)

				repeated_letters.add(l)

		return (w, total_info)


	def update_state(self, state, letter_info):
		"""
		Updates the letter-position state based on the evaluation of a letter

		Inputs:
		- current letter-position state
		- letter info containing:
			- letter
			- position
			- evaluation {3: correct, 2: present, 1: absent}

		Output: updated letter-position state
		"""
		l = self.letter_dict[letter_info[1]]
		position = letter_info[0]
		
		if letter_info[2] == 1:
			if letter_info[3]:
				state[position, l] = 1.
			else:
				state[:, l] = 1.

		if letter_info[2] == 2:
			state[:, l] = 2.
			# state[position, l] = 1.

		if letter_info[2] == 3:
			state[position, :] = 1.
			state[position, l] = 3.
		
		return state

	def sort_updates(updates):
		"""
		Rearrange letter infos for a sequence of state-updating that prevents new guesses to overwrite information from past guesses

		Input: Unordered sequence of updates

		Output: Ordered sequence of updates 
		"""
		sort_2 = [u for u in updates if u[2] == 2]
		sort_1 = [u for u in updates if u[2] == 1]
		sort_3 = [u for u in updates if u[2] == 3]

		return sort_2 + sort_1 + sort_3
	
	def get_state(self, guesses):
		"""
		Create letter-position state based on all current guesses

		Input: encoded guesses

		Output: letter-position state
		"""
		new_state = np.zeros((5, 26))
		all_update_guesses = []
		for g in guesses:

			# For specific scenario where a guess has 2 of the same letters and the truth only has one of the letters,
			# so the first letter guess is present and the update is sorted first
			# but the second letter guess is absent and overwrites the possibility of that letter being in the truth.
			present_letters = set()

			for i, l in enumerate(g):
				all_update_guesses.append((i, l[0], l[1], l[0] in present_letters))
				if l[1] == 2:
					all_update_guesses.append((i, l[0], 1, True))
					present_letters.add(l[0])

		sorted_updates = General_Solver.sort_updates(all_update_guesses)

		for info in sorted_updates:
			new_state = self.update_state(new_state, info)
		
		return new_state

	def vocab_filter_cycle(vocab, condition, p, l):
		"""
		Filters vocabulary based on the evaulation of one letter at one position

		Inputs:
		- current vocabulary
		- evaluation {3: correct, 2: present, 1: absent}
		- position of letter
		- letter

		Output: filtered vocabulary
		"""
		v = np.argmax(vocab, axis = -1)

		if condition == 1:
			return v[:, p] != l
		if condition == 2:
			return (v == l).sum(axis = 1).astype("bool")
		if condition == 3:
			return v[:, p] == l
		
		return np.arange(0, vocab.shape[0])

	def vocab_filter_total(state, vocab, pos_vocab):
		"""
		Full filter of vocabulary based on every letter-position combination in the letter-position state

		Inputs:
		- letter-position state
		- vocabulary
		- positionally encoded vocabulary

		Output: fully filtered vocabulary
		"""
		for p in range(state.shape[0]):
			for l in range(state.shape[1]):
				indices = General_Solver.vocab_filter_cycle(vocab, state[p, l], p, l)
				vocab = vocab[indices]
				pos_vocab = pos_vocab[indices]
		return vocab, pos_vocab
	

if __name__ == "__main__":
	dict_path = "/Users/yashwant/Library/Mobile Documents/com~apple~CloudDocs/yash_icloud/CSC 480/Wordle/five_letter_words.json"
	trisha = General_Solver(dict_path, daily = True)
	trisha.live_play_ultra()
