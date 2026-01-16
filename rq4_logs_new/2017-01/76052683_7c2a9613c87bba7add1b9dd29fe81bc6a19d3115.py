from utilities import timeit


def is_num(x):
    if x.lstrip('-').isdigit():
        return True
    return False


class Process(object):
    def __init__(self, instructions, a=7):
        self.registers = {'a' : a, 'b' : 0, 'c' : 0, 'd' : 0}
        self._make_lines(instructions)
        self.index = 0
        self.o_line = 'iteration: {}, index: {}, registers: {}'
        self.counts = {}
        self.counts['c'] = {2 : 0, 1 : 0, 0 : 0}
        self.counts['b'] = {2 : 0, 1 : 0, 0 : 0}
        self.prev = None

    def _make_lines(self, instructions):
        commands = {'cpy' : self._cpy,
                    'dec' : self._dec,
                    'jnz' : self._jnz,
                    'out' : self._out,
                    'inc' : self._inc}
        self.program = []
        for line in instructions.split('\n'):
            cmd, rest = line.split(' ', 1)
            if len(rest.split()) > 1:
                a, b = rest.split()
            else:
                a = rest
                b = ''

            self.program.append((commands[cmd], a, b))

    def _get_val(self, x):
        if is_num(x):
            return int(x)
        else:
            return self.registers[x]

    def _cpy(self, a, b):
        if b in self.registers:
            self.registers[b] = self._get_val(a)

    def _dec(self, a, *args):
        if a in self.registers:
            self.registers[a] -= 1

    def _jnz(self, a, b):
        value = self._get_val(a)
        jump = self._get_val(b)
        if value != 0:
            self.index += jump - 1

    def _out(self, a, *args):
        # if self.prev is None:
        #     self.prev = self.registers[a]
        # else:
        #     if self.prev == self.registers[a]:
        #         self.index += 100
        #         # print 'BROKEN SEQUENCE, {}'.format(self.counts),
        #     self.prev = self.registers[a]

        print self.registers[a],

    def _inc(self, a, *args):
        if self.index + 4 < len(self.program):
            self._increment(a)
        else:
            if a in self.registers:
                self.registers[a] += 1

    def _increment(self, a):
        # convert series of add/decrement loops to a multiplication
        if self.program[self.index + 1][0] is self._dec and \
           self.program[self.index + 2][0] is self._jnz and \
           self.program[self.index + 3][0] is self._dec and \
           self.program[self.index + 4][0] is self._jnz and \
           self.program[self.index - 1][0] is self._cpy:

            copy_source, copy_dest = self.program[self.index - 1][1:]
            first_dec = self.program[self.index + 1][1]
            jump_cond_1, jump_dist_1 = self.program[self.index + 2][1:]
            second_dec = self.program[self.index + 3][1]
            jump_cond_2, jump_dist_2 = self.program[self.index + 4][1:]

            if copy_dest == first_dec and \
                first_dec == jump_cond_1 and second_dec == jump_cond_2 and \
                int(jump_dist_1) == -2 and int(jump_dist_2) == -5:

                update = self.get_val(copy_source) * self.get_val(second_dec)
                self.registers[a] += update # a = c * d
                self.registers[first_dec] = 0 # c <- 0
                self.registers[second_dec] = 0 # d <- 0
                self.index += 4 #skip loop
        else:
            if a in self.registers:
                self.registers[a] += 1

    def _run_line(self):
        if self.index == 21:
            # print self.registers
            self.counts['c'][self.registers['c']] += 1
            self.counts['b'][self.registers['b']] += 1
        cmd, a, b = self.program[self.index]
        cmd(a, b)
        self.index += 1

        

    def run(self):
        iterations = 0
        while self.index < len(self.program):
            iterations += 1
            self._run_line()
            if iterations % 100000 == 0:
                # return 'good sequence: {}'.format(self.counts),
                print self.o_line.format(iterations, self.index, self.registers)
                return

@timeit
def get_results(instructions, part2=False):
    # for i in xrange(-2533, 300):
    #     t = Process(instructions, i)
    #     if t.run():
    #         print i + 2534
    # program(3)
    # print 2537/2
    t_val = 5
    t = Process(instructions, t_val - 2534)
    t.run()
    return 

if __name__ == '__main__':
    with open('instructions_day25.txt', 'r') as f:
        instructions = f.read().strip()
    print get_results(instructions, part2=False)
    # print get_results(instructions, part2=True)