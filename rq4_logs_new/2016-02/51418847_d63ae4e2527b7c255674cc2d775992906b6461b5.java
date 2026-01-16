
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.GridLayout;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.JButton;
import javax.swing.JFrame;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author De'Angelo
 */
public class Minesweeper implements ActionListener {

    JFrame frame = new JFrame("Minesweeper");
    JButton reset = new JButton("Reset");
    JButton[][] buttons = new JButton[20][20];
    int[][] counts = new int[20][20];
    Container grid = new Container();
    final int MINE = 10;

    public Minesweeper() {
        frame.setSize(1000, 800);
        frame.setLayout(new BorderLayout());
        frame.add(reset, BorderLayout.NORTH);
        reset.addActionListener(this);
        //Button Grid
        grid.setLayout(new GridLayout(20, 20));
        for (JButton[] button : buttons) {
            for (int b = 0; b < buttons[0].length; b++) {
                button[b] = new JButton();
                button[b].addActionListener(this);
                grid.add(button[b]);
            }
        }

        frame.add(grid, BorderLayout.CENTER);
            createRandomMines();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);

    }

    public static void main(String[] args) {
        new Minesweeper();

    }

    public void createRandomMines() {
        //initialize list of random pairs
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 0; x < counts.length; x++) {
            for (int y = 0; y < counts[0].length; y++) {
                list.add(x * 100 + y);

            }
        }
        //Reset counts, pick out 30 mines
        counts = new int[20][20];
        for (int a = 0; a < counts.length; a++) {
            int choice = (int) (Math.random() * list.size());
            counts[list.get(choice) / 100][list.get(choice) % 100] = MINE;
            list.remove(choice);

        }
        //Initialize neighbor counts
        for (int x = 0; x < counts.length; x++) {
            for (int y = 0; y < counts[0].length; y++) {
                if (counts[x][y] != MINE) {
                    int neighborcount = 0;
                    if (x > 0 && y > 0 && counts[x - 1][y - 1] == MINE) {//Up Left
                        neighborcount++;
                    }
                    if (y > 0 && counts[x][y - 1] == MINE) {//Up Left
                        neighborcount++;
                    }
                    if (x < counts.length - 1 && y < counts[0].length - 1 && counts[x + 1][y + 1] == MINE) { //Down Right
                        neighborcount++;
                    }
                    counts[x][y] = neighborcount;
                }

            }
        }
    }

    public void lostGame() {
        for (int x = 0; x < buttons.length; x++) {

            for (int y = 0; y < buttons[0].length; y++) {
                if (buttons[x][y].isEnabled()) {
                    if (counts[x][y] != MINE) {
                        buttons[x][y].setText(counts[x][y] + "");
                        buttons[x][y].setEnabled(false);
                    } else {
                        buttons[x][y].setText("X");
                        buttons[x][y].setEnabled(false);
                    }
                }
            }
        }
    }

    @Override
    public void actionPerformed(ActionEvent event) {
        if (event.getSource().equals(reset)) {
            //Reset The BOARD!!!!!
        } else {
            for (int x = 0; x < buttons.length; x++) {
                for (int y = 0; y < buttons[0].length; y++) {
                    if (event.getSource().equals(buttons[x][y])) {
                        if (counts[x][y] == MINE) {
                            buttons[x][y].setForeground(Color.red);
                            buttons[x][y].setText("X");
                            lostGame();
                        }
                        buttons[x][y].setText(counts[x][y] + "");
                        buttons[x][y].setEnabled(false);
                    }
                }
            }
        }

    }

    private LayoutManager newBorderLayout() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}