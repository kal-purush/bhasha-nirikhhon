class TuringCondition(object):

	def __init__(self, state, value):
		self.state = state
		self.value = value


class TuringCommand(object):

	def __init__(self, input, move, state):
		self.input = input
		self.move_dir = move
		self.new_state = state
	
	def is_empty(self):
		return not self.input and not self.move_dir and not self.new_state
	

class TuringTransition(object):

	def __init__(self, cond, comm):
		self.condition = cond
		self.command = comm			


def parse_transition(string):
	parts = string.split('->')
	state = ''
	value = ''
	input = ''
	move = ''
	new_state = ''
	cond_parts = parts[0].split(',')
	state = cond_parts[0]
	if len(cond_parts) > 1 and cond_parts[1]:
		value = cond_parts[1][0]
	if len(parts) > 1:
		comm_parts = parts[1].split(',')
		if comm_parts[0]:
			input = comm_parts[0][0]
		if len(comm_parts) > 1 and comm_parts[1]:
			move = comm_parts[1][0]
		if len(comm_parts) > 2 and comm_parts[2]:
			new_state = comm_parts[2]
	condition = TuringCondition(state, value)
	command = TuringCommand(input, move, new_state)
	return TuringTransition(condition, command)
	
def parse_transitions(text):		
	transitions = []
	lines = text.split('\n')
	for line in lines:
		if not line: continue
		transition = parse_transition(line)
		transitions.append(transition)
	return transitions
	

class TuringMachineStrip(object):
	
	empty_symbol = ' '
	
	def __init__(self):
		self.caret_position = 0
		self.cells = []
		self.reset()		
	
	def reset(self):
		self.cells = [self.empty_symbol] * 3
		self.caret_position = 1
	
	def write(self, symbol_value):
		self.cells[self.caret_position] = symbol_value
	
	def read(self):
		return self.cells[self.caret_position]
	
	def move(self, direction):
		if direction == 'L':
			if self.caret_position == len(self.cells) - 2 and \
			self.read() == self.empty_symbol:
				self.cells.pop()
			self.caret_position -= 1
			if self.caret_position == 0:
				self.cells.insert(0, self.empty_symbol)
				self.caret_position = 1
		if direction == 'R':
			if self.caret_position == 1 and \
			self.read() == self.empty_symbol:
				self.cells.remove(0)
				self.caret_position = 0
			self.caret_position += 1
			if self.caret_position == len(self.cells) - 1:
				self.cells.append(self.empty_symbol)
		pass
		
	def set_input(self, input_value):		
		self.cells = list(input_value)
		self.cells.insert(0, self.empty_symbol)
		self.cells.append(self.empty_symbol)
		self.caret_position = 1
	
	def get_output(self):
		output = ''
		start_read = False
		for cell in self.cells:
			if not start_read:
				if cell != self.empty_symbol:
					start_read = True
					output = cell
			else:
				if cell != self.empty_symbol:			
					output += cell
				else:
					break;
		return output
		

class TuringMachine(object):
	
	stop_state = '!'
	
	def __init__(self, states, init_state, transitions):
		self.strip = TuringMachineStrip()
		self.states = states
		self.initial_state = init_state
		self.transitions = transitions
		self.stop = False
		self.move_counter = 0
		
	def reset(self):
		output = self.strip.get_output()
		self.stop = False
		self.strip.reset()
		self.strip.set_input(output)
		self.state = self.initial_state		
		
	def set_input(self, input_value):
		self.strip.reset()
		self.strip.set_input(input_value)
		self.state = self.initial_state
		
	def get_next_transition(self):
		if not self.stop:
			for transition in self.transitions:
				condition = transition.condition
				if self.state == condition.state and self.strip.read() == condition.value:
					return transition
		return None
	
	def set_state(self, new_state):
		self.state = new_state
		if new_state == self.stop_state:
			self.stop = True
	
	def step(self):
		self.move_counter += 1	
		if self.move_counter > 100: 
			return False				
		transition = self.get_next_transition()
		if transition:
			command = transition.command
			if command.is_empty():
				self.stop = True
				return False
			if command.input:
				self.strip.write(command.input)
			if command.move_dir:
				self.strip.move(command.move_dir)
			if command.new_state:
				self.set_state(command.new_state)
			return True	
		return False
	
	def get_strip(self):
		return self.strip
	
	def get_output(self):
		if self.stop:
			return self.strip.get_output()
		return None	
		
	def run(self):
		self.move_counter = 0
		while not self.stop:			
			res = self.step()
			# print("%s %s" % (self.strip.caret_position, self.strip.cells))
			# x = input('pause...')			
			if not res:
				return False;
			if len(self.strip.cells) > 100:
				return False;
		return True	
		
		
if __name__ == "__main__":
	def get_states(transitions):
		states = []
		for transition in transitions:
			state = transition.condition.state
			if not state in states:
				states.append(state)
		return states
		
	def run_program(tur, input_word):
		transitions = parse_transitions(tur)
		states = get_states(transitions)
		m = TuringMachine(states, states[0], transitions)
		m.set_state(m.initial_state)
		m.set_input(input_word)	
				
		success = m.run()		
		if success:
			return m.get_output()
		else:
			raise 'Incorrect machine!'
			
	def test_program(tur, input_word, expect_result):
		try:
			res = run_program(tur, input_word)
			return res == expect_result
		except Exception as e:
			raise str(e)
			
	def run_test_set(tur, test_set):
		transitions = parse_transitions(tur)
		states = get_states(transitions)
		m = TuringMachine(states, states[0], transitions)
		total = len(test_set)
		counter = 0
		
		try:
			for test in test_set:
				input_word = test[0]
				expect_word = test[1]	
				m.reset()
				m.set_input(input_word)	
				result = m.run()
				if result:
					output_word = m.get_output()
					if output_word == expect_word:
						counter += 1
					else:
						raise Exception("%s/%s tests passed" % (counter, total))
				else:
					raise Exception("Runtime error")
			return "SUCCESS"
		except Exception as e:
			return str(e)
	
	tur = 'q0,0->,R,\nq0,1->,R,\nq0,2->,R,\nq0,3->,R,\nq0,4->,R,\nq0,5->,R,\nq0,6->,R,\nq0,7->,R,\nq0,8->,R,\nq0,9->,R,\nq0, ->,L,q1\nq1,0->1,,!\nq1,1->2,,!\nq1,2->3,,!\nq1,3->4,,!\nq1,4->5,,!\nq1,5->6,,!\nq1,6->7,,!\nq1,7->8,,!\nq1,8->9,,!\nq1,9->0,L,\nq1, ->1,,!'
	
	def parse_tests(line):
		res = []
		tests = line.split(';')
		for test in tests:
			if test and not test.isspace():
				spl = test.split(',')
				res.append((spl[0], spl[1],))
		return res
		
	tests = parse_tests('0,1;1,2;99,100;199,200;')	
		
	print(run_test_set(tur, tests))	
	
	
	
	