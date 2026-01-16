import copy
import itertools

def parse_input(fname):
    with open(fname) as f:
        data = f.readlines()
    return ExpandingList([int(x) for x in data[0].split(",")])

class ExpandingList(list):
    def __setitem__(self, index, value):
        if index >= len(self):
            self.extend([0] * (index + 1 - len(self)))
        list.__setitem__(self, index, value)

    def __getitem__(self, index):
        if isinstance(index, int):
            if index >= len(self):
                self.extend([0] * (index + 1 - len(self)))
            if index < 0:
                raise IndexError
        else:
            if index.stop and index.stop >= len(self):
                self.extend([0] * (index.stop + 1 - len(self)))
            if index.start and index.start < 0:
                raise IndexError

        return list.__getitem__(self, index)

class IntCodeComputer:
    def __init__(self, program, phase_setting=None, yield_at_output=False):
        self.program = program
        self.yield_at_output = yield_at_output
        self.pos = 0
        self.base = 0
        self.completed = True
        self.operation = None

        self.phase_setting = phase_setting
        self.value = None
        self.phase_set = phase_setting is None
        self.read_first_method = None
        self.read_second_method = None

        self.read_modes = {
            0: self.from_position,
            1: self.from_memory,
            2: self.from_relative_position,
        }

        self.set_modes = {
            0: self.to_position,
            2: self.to_relative_position,
        }

        self.operations = {
            1: self.add,
            2: self.multiply,
            3: self.save,
            4: self.output,
            5: self.jump_if_true,
            6: self.jump_if_false,
            7: self.less_than,
            8: self.equals,
            9: self.set_base,
        }
        self.verbose = False

    def run(self, input_signal):
        self.completed = True
        if not self.phase_set:
            self.value = self.phase_setting
        else:
            self.value = input_signal            

        while self.program[self.pos] != 99:
            self.update_mode()
            self.operation()

            if self.operation.__name__ == 'save':
                if not self.phase_set:
                    self.value = input_signal
                    self.phase_set = True

            if self.operation.__name__ == 'output':
                if self.yield_at_output:
                    self.completed = False
                    break

    def add(self):
        a, b, c = self.program[self.pos:self.pos+3]
        value = self.read_first_method(a) + self.read_second_method(b)
        self.set_value_method(pos=c, value=value)
        self.pos += 3

    def multiply(self):
        a, b, c = self.program[self.pos:self.pos+3]
        value = self.read_first_method(a) * self.read_second_method(b)
        self.set_value_method(pos=c, value=value)
        self.pos += 3

    def save(self):
        pos = self.read_first_method(self.pos)
        self.set_value_method(pos=pos, value=self.value)
        self.pos += 1

    def output(self):
        pos = self.read_first_method(self.pos)
        self.value = self.program[pos]
        if self.verbose:
            print("Output value: {}".format(self.value))
        self.pos += 1

    def jump_if_true(self):
        a, b, c = self.program[self.pos:self.pos+3]
        a = self.read_first_method(a)
        b = self.read_second_method(b)
        if a:
            self.pos = b
        else:
            self.pos += 2

    def jump_if_false(self):
        a, b, c = self.program[self.pos:self.pos+3]
        a = self.read_first_method(a)
        b = self.read_second_method(b)
        if not a:
            self.pos = b
        else:
            self.pos += 2

    def less_than(self):
        a, b, c = self.program[self.pos:self.pos+3]
        a = self.read_first_method(a)
        b = self.read_second_method(b)
        if a < b:
            value = 1
        else:
            value = 0
        self.set_value_method(pos=c, value=value)
        self.pos += 3

    def equals(self):
        a, b, c = self.program[self.pos:self.pos+3]
        a = self.read_first_method(a)
        b = self.read_second_method(b)
        if a == b:
            value = 1
        else:
            value = 0
        self.set_value_method(pos=c, value=value)
        self.pos += 3

    def set_base(self):
        pos = self.read_first_method(self.pos)
        self.base = self.program[pos]
        self.pos += 1

    def from_position(self, pos):
        return self.program[pos]

    def from_relative_position(self, pos):
        return self.program[pos + self.base]

    def from_memory(self, pos):
        return pos

    def to_position(self, pos, value):
        self.program[pos] = value

    def to_relative_position(self, pos, value):
        self.program[pos + self.base] = value

    def update_mode(self):
        code = str(self.program[self.pos])

        if len(code) == 5:
            a = int(code[0])
            code = code[1:]
        else:
            a = 0
        self.set_value_method = self.set_modes[a]

        if len(code) == 4:
            b = int(code[0])
            code = code[1:]
        else:
            b = 0
        self.read_second_method = self.read_modes[b]

        if len(code) == 3:
            c = int(code[0])
            code = code[1:]
        else:
            c = 0
        self.read_first_method = self.read_modes[c]

        op = int(code)

        # back hack.. If it's a save method, the setting applies
        # to the final mode instead of the first. maybe.
        if op == 3:
            self.set_value_method = self.set_modes[c]

        self.operation = self.operations[op]
        self.pos += 1

def run(intcodes, input_signal=0, phase_setting=None, pos=0, yield_at_output=False):
    computer = IntCodeComputer(intcodes, phase_setting=phase_setting, yield_at_output=yield_at_output)
    computer.run(input_signal)

    return computer.program, computer.value, computer.pos, computer.completed

def run_tests():

    for input_signal in range(-10,10):
        # Positional mode
        assert (input_signal == 8) == run([3,9,8,9,10,9,4,9,99,-1,8], input_signal=input_signal)[1]
        assert (input_signal < 8) == run([3,9,7,9,10,9,4,9,99,-1,8], input_signal=input_signal)[1]

        # Immediate mode
        assert (input_signal == 8) == run([3,3,1108,-1,8,3,4,3,99], input_signal=input_signal)[1]
        assert (input_signal < 8) == run([3,3,1107,-1,8,3,4,3,99], input_signal=input_signal)[1]

        # Jump mode
        assert (input_signal != 0) == run([3,12,6,12,15,1,13,14,13,4,13,99,-1,0,1,9], input_signal=input_signal)[1]
        assert (input_signal != 0) == run([3,3,1105,-1,9,1101,0,0,12,4,12,99,1], input_signal=input_signal)[1]

        # Larger example
        program = [3,21,1008,21,8,20,1005,20,22,107,8,21,20,1006,20,31,
                   1106,0,36,98,0,0,1002,21,125,20,4,20,1105,1,46,104,
                   999,1105,1,46,1101,1000,1,20,4,20,1105,1,46,98,99]
        output = run(program, input_signal=input_signal)[1]

        if input_signal < 8:
            assert output == 999
        elif input_signal == 8:
            assert output == 1000
        else:
            assert output == 1001

    original_data = parse_input("day5/input.txt")
    input_signal = run(original_data, input_signal=1)[1]
    assert input_signal == 13210611

    original_data = parse_input("day5/input.txt")
    input_signal = run(original_data, input_signal=5)[1]
    assert input_signal == 584126

if __name__ == '__main__':
    run_tests()