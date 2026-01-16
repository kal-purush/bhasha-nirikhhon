import itertools
from copy import deepcopy

BOX = 'box'
ROW = 'row'
COL = 'col'

class SudokuError(Exception):
    def __init__(self, message):
        self.message = message


class Sudoku(object):
    def __init__(self, filename):
        self.header = '''\\documentclass[10pt]{article}
\\usepackage[left=0pt,right=0pt]{geometry}
\\usepackage{tikz}
\\usetikzlibrary{positioning}
\\usepackage{cancel}
\\pagestyle{empty}

\\newcommand{\\N}[5]{\\tikz{\\node[label=above left:{\\tiny #1},
                               label=above right:{\\tiny #2},
                               label=below left:{\\tiny #3},
                               label=below right:{\\tiny #4}]{#5};}}

\\begin{document}

\\tikzset{every node/.style={minimum size=.5cm}}

\\begin{center}
\\begin{tabular}{||@{}c@{}|@{}c@{}|@{}c@{}||@{}c@{}|@{}c@{}|@{}c@{}||@{}c@{}|@{}c@{}|@{}c@{}||}\\hline\\hline
'''
        self.template = '''% Line 1
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline

% Line 2
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline

% Line 3
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline\\hline

% Line 4
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline

% Line 5
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline

% Line 6
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline\\hline

% Line 7
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline

% Line 8
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline

% Line 9
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} &
\\N{}{}{}{}{} & \\N{}{}{}{}{} & \\N{}{}{}{}{} \\\ \\hline\\hline
'''
        self. footer = '''\end{tabular}
\end{center}

\end{document}
'''

        self.filename = filename.split('.txt')[0]
        try:
            with open(file=filename, mode='r') as f:
                content = f.read().replace(' ', '').split(sep='\n')
                lines = content.copy()
                [lines.remove(line) for line in content if not line]

                grid = []
                for line in lines:
                    grid.append([int(x) for x in list(line)])
                self.grid = grid
                self.grid_tran = list(zip(*self.grid))
                self.boxes = self.split_to_boxes()
                self.find_possibilities()

                self.forced = False
                self.marked = False

                # print(self.possibilities)
        except Exception as e:
            raise SudokuError('Incorrect input')

    '''
   0  0  1  9  0  0  0  0  8     
   6  0  0  0  8  5  0  3  0     
   0  0  7  0  6  0  1  0  0     
   0  3  4  0  9  0  0  0  0     
   0  0  0  5  0  4  0  0  0     
   0  0  0  0  1  0  4  2  0     
   0  0  5  0  7  0  9  0  0
   0  1  0  8  4  0  0  0  7
   7  0  0  0  0  9  2  0  0                  '''


    def find_possibilities(self):
        possibilties = []

        for i in range(9):
            row_possibilities = []
            for j in range(9):
                box_num = 3*(i//3) + j//3
                tmp = set()
                elem = self.grid[i][j]
                if elem == 0:
                    for p in range(1, 10):
                        # print(i, j, box_num)
                        if p not in self.grid[i] and p not in self.grid_tran[j] and p not in self.boxes[box_num]:
                            tmp.add(p)
                row_possibilities.append(tmp)
            possibilties.append(row_possibilities)
        self.possibilities = possibilties
        self.possibilities_tran = [list(x) for x in list(zip(*self.possibilities))]
        self.possibilities_boxes = self.split_to_boxes(self.possibilities)

    def find_poss_combos(self):
        poss_combos = []
        for i in range(9):
            row_combos = []
            for j in range(9):
                cell_combos = []
                poss = tuple(self.possibilities[i][j])
                cell_combos.append(poss)
                for size in range(2, len(poss)):
                    cell_combos.extend(list(itertools.combinations(poss, size)))
                cell_combos = [set(x) for x in cell_combos]
                row_combos.append(cell_combos)

            poss_combos.append(row_combos)
        self.poss_combos = poss_combos
        self.poss_combos_tran = [list(x) for x in list(zip(*poss_combos))]
        self.poss_combos_boxes = self.split_to_boxes(poss_combos)


    def split_to_boxes(self, array=None):
        array = array or self.grid
        boxes = []
        for i in (0, 3, 6):

            for j in (0, 3, 6):
                tmp = []
                tmp.extend(array[i][j : j+3])
                tmp.extend(array[i+1][j : j + 3])
                tmp.extend(array[i+2][j : j + 3])
                boxes.append(tmp)
        return boxes

    def preassess(self):
        # print(self.grid)

        if len(self.grid_tran) != 9:
            # print(grid_tran)
            raise SudokuError('Incorrect input')
        no_soln = False
        for line in self.grid:
            for elem in set(line):
                if elem > 0 and line.count(elem) > 1:
                    no_soln = True
                    print('There is clearly no solution.')
                    break
        if not no_soln:
            for line in self.grid_tran:
                for elem in set(line):
                    if elem > 0 and line.count(elem) > 1:
                        no_soln = True
                        print('There is clearly no solution.')
                        break
        if not no_soln:
            for line in self.boxes:
                for elem in set(line):
                    if elem > 0 and line.count(elem) > 1:
                        no_soln = True
                        print('There is clearly no solution.')
                        break
        if not no_soln:
            print('There might be a solution.')

    def bare_tex_output(self):
        vals = ['{}']*5
        cons_vals = []
        for line in self.grid:
            for x in line:
                if x > 0:
                    vals[-1] = '{' + str(x) + '}'
                cons_vals.extend(vals)
                vals[-1] = '{}'
        with open(self.filename+'_bare.tex', 'w') as f:
            f.write(self.header)
            f.write(self.template.format(*cons_vals))
            f.write(self.footer)

    def forced_tex_output(self):
        grid_modified = False
        for i in range(9):
            for j in range(9):
                # if i==8 and j==3:
                #     print(self.possibilities)
                elem = self.grid[i][j]
                possibilities = self.possibilities[i][j].copy()
                if elem == 0:
                    if len(possibilities) == 1:
                        self.possibilities[i][j].pop()
                        # print('pop', self.possibilities[i][j])
                        poss = possibilities.pop()
                        # print('poss', poss)
                        self.grid[i][j] = poss
                        self.grid_tran = list(zip(*self.grid))
                        self.boxes = self.split_to_boxes()
                        grid_modified = True
                        self.find_possibilities()
                        # print('grid_poss', i, j, poss, self.grid[i][j])
                        # [self.possibilities[i][r].discard(poss) for r in range(9)]
                        # sel
                        #
                        #
                        # f.possibilities_tran = [list(x) for x in list(zip(*self.possibilities))]
                        # self.possibilities_boxes = self.split_to_boxes(self.possibilities)
                        # [self.possibilities_tran[i][r].discard(poss) for r in range(9)]
                        # [self.possibilities[i][r].discard(poss) for r in range(9)]
                        # self.forced_tex_output()
                    else:
                        b = 3*(i//3) + j//3
                        row_possibilities = self.possibilities[i].copy()
                        row_possibilities.remove(possibilities)
                        col_possibilities = self.possibilities_tran[j].copy()
                        # print(elem, i, j)
                        # print(self.possibilities)
                        # print(possibilities, col_possibilities)
                        # print(row_possibilities)
                        col_possibilities.remove(possibilities)
                        box_possibilities = self.possibilities_boxes[b].copy()
                        box_possibilities.remove(possibilities)
                        for poss in possibilities:
                            row_poss = set()
                            col_poss = set()
                            box_poss = set()
                            [row_poss.update(x) for x in row_possibilities]
                            [col_poss.update(x) for x in col_possibilities]
                            [box_poss.update(x) for x in box_possibilities]
                            if poss not in box_poss: #or poss not in col_poss or poss not in row_poss:
                                self.grid[i][j] = poss
                                self.grid_tran = list(zip(*self.grid))
                                self.boxes = self.split_to_boxes()
                                grid_modified = True
                                self.find_possibilities()
                                # self.possibilities[i][j] = set()
                                # [self.possibilities[i][r].discard(poss) for r in range(9)]
                                # self.possibilities_tran = [list(x) for x in list(zip(*self.possibilities))]
                                # self.possibilities_boxes = self.split_to_boxes(self.possibilities)
                                # [self.possibilities_tran[j][r].discard(poss) for r in range(9)]
                                # [self.possibilities_boxes[b][r].discard(poss) for r in range(9)]
                                # self.forced_tex_output()

        if grid_modified:
            self.forced_tex_output()
            # print(self.grid)
        else:
            with open(self.filename + '_forced.tex', 'w') as f:
                vals = ['{}']*5
                cons_vals = []
                for i in range(9):
                    for j in range(9):
                        x = self.grid[i][j]
                        if x > 0:
                            vals[-1] = '{' + str(x) + '}'
                        cons_vals.extend(vals)
                        vals[-1] = '{}'
                f.write(self.header)
                f.write(self.template.format(*cons_vals))
                f.write(self.footer)
            self.forced = True
            # print('\n\n\n')
            # for line in self.possibilities:
            #     print(line)
            # for line in self.grid:
            #     print(line)

    def marked_tex_output(self):
        if not self.forced:
            self.forced_tex_output()
        with open(self.filename + '_marked.tex', 'w') as f:
            vals = ['{}'] * 5
            cons_vals = []
            for i in range(9):
                for j in range(9):
                    x = self.grid[i][j]
                    if x > 0:
                        vals[-1] = '{' + str(x) + '}'
                    else:
                        poss = self.possibilities[i][j]
                        for k in range(1, 10):
                            if k in poss:
                                idx = 3 if k == 9 else (k-1) // 2
                                # print(k, idx)
                                vals[idx] = '{' + str(k) + '}' if vals[idx] == '{}' else vals[idx].split('}')[0] + ' ' + str(k) + '}'
                    # print(vals)
                    cons_vals.extend(vals)
                    vals = ['{}']*5
            f.write(self.header)
            f.write(self.template.format(*cons_vals))
            f.write(self.footer)
        self.possibilities_after_marking = deepcopy(self.possibilities)
        self.marked = True


    def is_preemptive(self, r, c):
        poss = self.possibilities[r][c]
        rcb = {ROW: False, COL: False, BOX: False}
        poss_comb = self.poss_combos[r][c]
        row_poss = self.possibilities[r]
        col_poss = self.possibilities_tran[c]
        b = 3 * (r // 3) + c // 3
        box_poss = self.possibilities_boxes[b]
        row_match = []
        col_match = []
        box_match = []

        [box_match.append(i) for i, x in enumerate(box_poss) if x in poss_comb]
        if len(box_match) == len(poss):
            rcb[BOX] = True

        [row_match.append(i) for i, x in enumerate(row_poss) if x in poss_comb]
        if len(row_match) == len(poss):
            rcb[ROW] = True

        [col_match.append(i) for i, x in enumerate(col_poss) if x in poss_comb]
        if len(col_match) == len(poss):
            rcb[COL] = True

        return rcb, {ROW: row_match, COL: col_match, BOX: box_match}

    def handle_single_poss(self, r, c):
        poss = self.possibilities[r][c].pop()
        # print('pop', self.possibilities[i][j])
        # print('poss', poss)
        self.grid[r][c] = poss
        self.grid_tran = list(zip(*self.grid))
        self.boxes = self.split_to_boxes()
        for i in range(9):
            if poss in self.possibilities[r][i]:
                self.possibilities[r][i].remove(poss)
                if len(self.possibilities[r][i]) == 1:
                    self.handle_single_poss(r, i)
            if poss in self.possibilities[i][c]:
                self.possibilities[i][c].remove(poss)
                if len(self.possibilities[i][c]) == 1:
                    self.handle_single_poss(i, c)
        for b_i in range(r // 3 * 3, r // 3 * 3 + 3):
            for b_j in range(c // 3 * 3, c // 3 * 3 + 3):
                if poss in self.possibilities[b_i][b_j]:
                    self.possibilities[b_i][b_j].remove(poss)
                    if len(self.possibilities[b_i][b_j]) == 1:
                        self.handle_single_poss(b_i, b_j)


    def worked_tex_output(self):
        if not self.marked:
            self.marked_tex_output()
        self.find_poss_combos()

        grid_changed = False
        for i in range(9):
            for j in range(9):
                poss = self.possibilities[i][j]
                b = 3 * (i // 3) + j // 3
                if self.grid[i][j] == 0:
                    res = self.is_preemptive(i, j)
                    if res:

                        rcb = res[0]
                        if rcb[ROW]:
                            matches = res[1][ROW]
                            for col in range(9):
                                if col not in matches and self.grid[i][col]==0:
                                    if self.possibilities[i][col] != set(self.possibilities[i][col]).difference(poss):
                                        grid_changed = True
                                        self.possibilities[i][col] = set(self.possibilities[i][col]).difference(poss)
                                        if len(self.possibilities[i][col]) == 1:
                                            self.handle_single_poss(i, col)

                        if rcb[COL]:
                            matches = res[1][COL]
                            for row in range(9):
                                if row not in matches and self.grid[row][j]==0:
                                    if self.possibilities[row][j] != set(self.possibilities[row][j]).difference(poss):
                                        grid_changed = True
                                        self.possibilities[row][j] = set(self.possibilities[row][j]).difference(poss)
                                        if len(self.possibilities[row][j]) == 1:
                                            self.handle_single_poss(row, j)

                        if rcb[BOX]:
                            matches = res[1][BOX]
                            idx = 0
                            for row in range(i//3 * 3, i//3 * 3 + 3):
                                for col in range(j//3 * 3, j//3 * 3 + 3):
                                    if idx not in matches and self.grid[row][col]==0:
                                        if self.possibilities[row][col] != set(self.possibilities[row][col]).difference(poss):
                                            grid_changed = True
                                            self.possibilities[row][col] = set(
                                                self.possibilities[row][col]).difference(poss)
                                            if len(self.possibilities[row][col]) == 1:
                                                self.handle_single_poss(row, col)
                                    idx += 1

                        self.possibilities_tran = [list(x) for x in list(zip(*self.possibilities))]
                        self.possibilities_boxes = self.split_to_boxes(self.possibilities)
                        self.find_poss_combos()

        if grid_changed:
            self.worked_tex_output()
        else:
            # TODO add writing to tex file
            # for i in range(9):
            #     for j in range(9):
            #         if self.grid[i][j] == 0:
            #             print(i, j, self.grid[i][j], self.possibilities[i][j])
            # print()
            # [print(x) for x in self.possibilities]
            # [print(x) for x in self.grid]

            cancelled_possibilities = deepcopy(self.possibilities)
            for i in range(9):
                for j in range(9):
                    cancelled_possibilities[i][j] = self.possibilities_after_marking[i][j].difference(self.possibilities[i][j])
            # print(cancelled_possibilities)
            with open(self.filename + '_worked.tex', 'w') as f:
                vals = ['{}'] * 5
                cons_vals = []
                for i in range(9):
                    for j in range(9):
                        x = self.grid[i][j]
                        if x > 0:
                            vals[-1] = '{' + str(x) + '}'
                        poss = self.possibilities[i][j]
                        cancelled_poss = cancelled_possibilities[i][j]
                        CANCEL = '\cancel'
                        for k in range(1, 10):
                            idx = 3 if k == 9 else (k - 1) // 2
                            if k in poss:
                                # print(k, idx)
                                vals[idx] = '{' + str(k) + '}' if vals[idx] == '{}' else vals[idx].split('}')[
                                                                                             0] + ' ' + str(k) + '}'
                            if k in cancelled_poss:
                                # print(k, idx)
                                tmp_cancel = CANCEL + '{' + str(k) + '}'
                                vals[idx] = '{' + tmp_cancel + '}' if vals[idx] == '{}' else vals[idx][:-1] + ' ' + tmp_cancel + '}'
                        # print(vals)
                        cons_vals.extend(vals)
                        vals = ['{}'] * 5
                f.write(self.header)
                f.write(self.template.format(*cons_vals))
                f.write(self.footer)
            



# s = Sudoku('sudoku_3.txt')
# s.preassess()
# s.bare_tex_output()
# s.worked_tex_output()
