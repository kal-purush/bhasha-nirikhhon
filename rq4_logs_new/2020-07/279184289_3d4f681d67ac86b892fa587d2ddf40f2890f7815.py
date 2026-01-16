import matplotlib.pyplot as plt
import numpy as np

def setup_grid():
    fig = plt.figure()

    plt.xlim(0,3)
    plt.ylim(0,3)
    
    for x in [1,2]:
        plt.vlines(x = x, ymin = 0, ymax = 3)
    
    for y in [1,2]:
        plt.hlines(y = y, xmin = 0, xmax = 3)
    
    plt.axis('off')

def add_elements(board):
    for i in range(3):
        for j in range(3):
            if board[j][i] == 1:
                plt.plot([i + 0.5 - 0.4, i + 0.5 + 0.4],
                        [2 - j + 0.5 + 0.4, 2 - j + 0.5 - 0.4], color = 'black')
                plt.plot([i + 0.5 - 0.4, i + 0.5 + 0.4],
                        [2 - j + 0.5 - 0.4, 2 - j + 0.5 + 0.4], color = 'black')

            if board[j][i] == -1:
                theta = np.linspace(0,1,200)
                x = 0.4*np.cos(2*np.pi*theta) + i + 0.5
                y = 0.4*np.sin(2*np.pi*theta) + 2 - j + 0.5

                plt.plot(x,y, color = 'black')

def draw_board(board):
    setup_grid()
    add_elements(board)
    plt.show(block = False)

if __name__ == '__main__':
    draw_board([[0,-1,0],[1,1,0],[0,0,0]])
    input('Press Enter to close plot.')