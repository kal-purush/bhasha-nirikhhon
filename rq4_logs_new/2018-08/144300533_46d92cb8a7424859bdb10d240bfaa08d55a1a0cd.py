import csv

class Sudoku:

    def __init__(self, board_file_location):
        self.board = []
        with open(board_file_location, newline='') as csvfile:
            linereader = csv.reader(csvfile, delimiter=',')
            for row in linereader:
                self.board.append([int(i) for i in row])

    def __str__(self):
        s = ' '
        s += "_ "*3 + ' ' + "_ "*3 + ' ' + "_ "*3 + '\n'
        for i in range(0, len(self.board)):
            if i in [3,6]:
                s += ' '
                s += "_ "*3 + ' ' + "_ "*3 + ' ' + "_ "*3 + '\n'
            s += '|'
            for j in range(0, len(self.board)):
                if j in [2,5,8]:
                    s += str(self.board[i][j]) + '| '
                else:
                    s += str(self.board[i][j]) + ' '
            s += '\n'
            if i in [2,5,8]:
                s += ' '
                s += "‾ "*3 + ' ' + "‾ "*3 + ' ' + "‾ "*3 + '\n'
        return s

    def solve(self):
        pass

def main():
    s = Sudoku("./sudoku_easy.csv")
    print(s)

if __name__ == "__main__":
    main()