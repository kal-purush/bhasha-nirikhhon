class Grammar:
    def __init__(self, N, E, P, S):
        self.N = N
        self.E = E
        self.P = P
        self.S = S
        print(self.N, self.E, self.P, self.S)

    def get_nonterminals(self):
        return self.N

    def get_terminals(self):
        return self.E

    def print_set_of_productions(self):
        print("Set of productions:")
        for p in self.P:
            print(p[0], end=" ")
            for i in range(1, len(p)):
                print(p[i], end=" ")
            print()

    def print_productions_for_non_terminal(self, n):
        print("Productions for nonterminal " + n + ":")
        for key in self.P.keys():
            if key == n:
                print(self.P[key])

    def get_productions_for_non_terminal(self, n):
        return self.P[n]

    def check_if_context_free_grammar(self):
        for p in self.P:
            for i in range(1, len(p)):
                if p[i] not in self.N and p[i] not in self.E:
                    return False
        return True