class Checker:
    def __init__(self, matryca):
        self.matryca = matryca

    def check_win(self):
        for row in self.matryca:
            row_str = ''.join([p[0] for p in row])
            if 'GGG' in row_str:
                return "Zielony wygrywa!"
            elif 'RRR' in row_str:
                return "Czerwony wygrywa!"

        for i in range(len(self.matryca[0])):
            col_str = ''.join([self.matryca[j][i][0] for j in range(len(self.matryca))])
            if 'GGG' in col_str:
                return "Zielony wygrywa!"
            elif 'RRR' in col_str:
                return "Czerwony wygrywa!"

        return "Nie mamy jeszcze zwycięzcy."