import javax.swing.*;
import java.awt.*;
import java.awt.event.*;


public class TicTacToe extends JFrame implements ActionListener {

    private final char EMPTY = ' ';
    private char[][] board;
    private char currentPlayer;
    private JButton[][] buttons;

    public TicTacToe() {
        setTitle("Tic Tac Toe");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new GridLayout(3, 3));

        board = new char[3][3];
        currentPlayer = 'X';
        buttons = new JButton[3][3];

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                buttons[i][j] = new JButton("");
                buttons[i][j].addActionListener(this);
                add(buttons[i][j]);
                board[i][j] = EMPTY;
            }
        }

        setVisible(true);
        setSize(500 , 400);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        JButton clickedButton = (JButton) e.getSource();
        int row = 0;
        int col = 0;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                if (clickedButton == buttons[i][j]) {
                    row = i;
                    col = j;
                    break;
                }
            }
        }

        if (board[row][col] == EMPTY) {
            board[row][col] = currentPlayer;
            clickedButton.setText(String.valueOf(currentPlayer));

            if (checkWinner(currentPlayer)) {
                JOptionPane.showMessageDialog(this, "Player " + currentPlayer + " Wins!");
                resetGame();
            } else if (isBoardFull()) {
                JOptionPane.showMessageDialog(this, "It's a Tie!");
                resetGame();
            }

            currentPlayer = currentPlayer == 'X' ? 'O' : 'X';
        }
    }

    private boolean checkWinner(char player) {
        // Check rows
        for (int i = 0; i < 3; i++) {
            if (board[i][0] == player && board[i][1] == player && board[i][2] == player) {
                return true;
            }
        }

        // Check columns
        for (int i = 0; i < 3; i++) {
            if (board[0][i] == player && board[1][i] == player && board[2][i] == player) {
                return true;
            }
        }

        // Check diagonals
        if (board[0][0] == player && board[1][1] == player && board[2][2] == player) {
            return true;
        }
        if (board[0][2] == player && board[1][1] == player && board[2][0] == player) {
            return true;
        }

        return false;
    }

    private boolean isBoardFull() {
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                if (board[i][j] == EMPTY) {
                    return false;
                }
            }
        }
        return true;
    }

    private void resetGame() {
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                board[i][j] = EMPTY;
                buttons[i][j].setText("");
            }
        }
        currentPlayer = 'X';
    }

    public static void main(String[] args) {
        new TicTacToe();
    }
}