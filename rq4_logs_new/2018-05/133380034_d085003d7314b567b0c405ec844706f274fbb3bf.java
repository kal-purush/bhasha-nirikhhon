//original sample code,visit at: javatpoint.com/Puzzle-Game
import java.awt.*;
import java.awt.event.*;
import java.util.Random;
import java.util.Stack;
import javax.swing.*;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoManager;

public class Puzzle16 extends JFrame implements ActionListener {

    JButton b1, b2, b3, b4, b5, b6, b7, b8, b9, ref, b10, b11, b12, b13, b14, b15, b16, res, undo, redo;
    static int count = 0;
    JLabel lb, lb2;
    long before, after;
    UndoManager manager = new UndoManager();
    Stack undoStack, redoStack;
    randomBoard board = new randomBoard();
    String[] arr = new String[16];
    String[] sample = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"};
    String[] obs = {"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "X", "X", "0", ""};
    String[] result = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"};

    Puzzle16() {
        super("Puzzle 4 * 4 Game");
        int run = 0;

        board.randomize(obs);
        board.mergeObs(sample, obs, arr);
        board.randomizeNum(arr);
        board.boardResult(result, obs);
        lb = new JLabel();
        lb.setBounds(470, 50, 180, 20);
        lb2 = new JLabel();
        lb2.setBounds(470, 100, 180, 20);
        b1 = new JButton(arr[0]);
        b1.setBounds(50, 50, 100, 100);
        b2 = new JButton(arr[1]);
        b2.setBounds(150, 50, 100, 100);
        b3 = new JButton(arr[2]);
        b3.setBounds(250, 50, 100, 100);
        b4 = new JButton(arr[3]);
        b4.setBounds(350, 50, 100, 100);

        b5 = new JButton(arr[4]);
        b5.setBounds(50, 150, 100, 100);
        b6 = new JButton(arr[5]);
        b6.setBounds(150, 150, 100, 100);
        b7 = new JButton(arr[6]);
        b7.setBounds(250, 150, 100, 100);
        b8 = new JButton(arr[7]);
        b8.setBounds(350, 150, 100, 100);

        b9 = new JButton(arr[8]);
        b9.setBounds(50, 250, 100, 100);
        b10 = new JButton(arr[9]);
        b10.setBounds(150, 250, 100, 100);
        b11 = new JButton(arr[10]);
        b11.setBounds(250, 250, 100, 100);
        b12 = new JButton(arr[11]);
        b12.setBounds(350, 250, 100, 100);

        b13 = new JButton(arr[12]);
        b13.setBounds(50, 350, 100, 100);
        b14 = new JButton(arr[13]);
        b14.setBounds(150, 350, 100, 100);
        b15 = new JButton(arr[14]);
        b15.setBounds(250, 350, 100, 100);
        b16 = new JButton(arr[15]);
        b16.setBounds(350, 350, 100, 100);

        ref = new JButton("New game");
        ref.setBounds(50, 650, 400, 100);

        res = new JButton("Result");
        res.setBounds(50, 550, 400, 100);

        undo = new JButton("Back");
        undo.setBounds(50, 450, 200, 100);
        undo.setEnabled(false);

        redo = new JButton("Forward");
        redo.setBounds(250, 450, 200, 100);
        redo.setEnabled(false);

        b1.addActionListener(this);
        b2.addActionListener(this);
        b3.addActionListener(this);
        b4.addActionListener(this);
        b5.addActionListener(this);
        b6.addActionListener(this);
        b7.addActionListener(this);
        b8.addActionListener(this);
        b9.addActionListener(this);
        b10.addActionListener(this);
        b11.addActionListener(this);
        b12.addActionListener(this);
        b13.addActionListener(this);
        b14.addActionListener(this);
        b15.addActionListener(this);
        b16.addActionListener(this);
        ref.addActionListener(this);
        res.addActionListener(this);
        add(lb);
        add(lb2);
        add(b1);
        add(b2);
        add(b3);
        add(b4);
        add(b5);
        add(b6);
        add(b7);
        add(b8);
        add(b9);
        add(b10);
        add(b11);
        add(b12);
        add(b13);
        add(b14);
        add(b15);
        add(b16);
        add(ref);
        add(res);
        add(undo);
        add(redo);
        setSize(1000, 1000);
        setLayout(null);
        setVisible(true);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
    }

    public void actionPerformed(ActionEvent e) {
        if (count == 0) {
            before = System.currentTimeMillis();
        }
        lb.setText("Total Clicks: " + (++count));

        if (e.getSource() == b1) {
            String label1 = b1.getText();
            if (label1 != "X") {
                if (b2.getText().equals("")) {
                    b2.setText(label1);
                    b1.setText("");
                    undoStack.push(label1);
                }
                if (b5.getText().equals("")) {
                    b5.setText(label1);
                    b1.setText("");
                }
            }
        }//end of b1
        if (e.getSource() == b2) {
            String label2 = b2.getText();
            if (label2 != "X") {
                if (b1.getText().equals("")) {
                    b1.setText(label2);
                    b2.setText("");
                }
                if (b3.getText().equals("")) {
                    b3.setText(label2);
                    b2.setText("");
                }
                if (b6.getText().equals("")) {
                    b6.setText(label2);
                    b2.setText("");
                }
            }
        }//end of b2
        if (e.getSource() == b3) {
            String label3 = b3.getText();
            if (label3 != "X") {
                if (b2.getText().equals("")) {
                    b2.setText(label3);
                    b3.setText("");
                }
                if (b4.getText().equals("")) {
                    b4.setText(label3);
                    b3.setText("");
                }
                if (b7.getText().equals("")) {
                    b7.setText(label3);
                    b3.setText("");
                }
            }
        }//end of b3
        if (e.getSource() == b4) {
            String label4 = b4.getText();
            if (label4 != "X") {
                if (b3.getText().equals("")) {
                    b3.setText(label4);
                    b4.setText("");
                }
                if (b8.getText().equals("")) {
                    b8.setText(label4);
                    b4.setText("");
                }
            }
        }//end of b4
        if (e.getSource() == b5) {
            String label5 = b5.getText();
            if (label5 != "X") {
                if (b1.getText().equals("")) {
                    b1.setText(label5);
                    b5.setText("");
                }
                if (b6.getText().equals("")) {
                    b6.setText(label5);
                    b5.setText("");
                }
                if (b9.getText().equals("")) {
                    b9.setText(label5);
                    b5.setText("");
                }
            }
        }//end of b5
        if (e.getSource() == b6) {
            String label6 = b6.getText();
            if (label6 != "X") {
                if (b2.getText().equals("")) {
                    b2.setText(label6);
                    b6.setText("");
                }
                if (b5.getText().equals("")) {
                    b5.setText(label6);
                    b6.setText("");
                }
                if (b7.getText().equals("")) {
                    b7.setText(label6);
                    b6.setText("");
                }
                if (b10.getText().equals("")) {
                    b10.setText(label6);
                    b6.setText("");
                }
            }
        }//end of b6
        if (e.getSource() == b7) {
            String label7 = b7.getText();
            if (label7 != "X") {
                if (b3.getText().equals("")) {
                    b3.setText(label7);
                    b7.setText("");
                }
                if (b6.getText().equals("")) {
                    b6.setText(label7);
                    b7.setText("");
                }
                if (b8.getText().equals("")) {
                    b8.setText(label7);
                    b7.setText("");
                }
                if (b11.getText().equals("")) {
                    b11.setText(label7);
                    b7.setText("");
                }
            }
        }//end of b7
        if (e.getSource() == b8) {
            String label8 = b8.getText();
            if (label8 != "X") {
                if (b4.getText().equals("")) {
                    b4.setText(label8);
                    b8.setText("");
                }
                if (b7.getText().equals("")) {
                    b7.setText(label8);
                    b8.setText("");
                }
                if (b12.getText().equals("")) {
                    b12.setText(label8);
                    b8.setText("");
                }
            }
        }//end of b8
        if (e.getSource() == b9) {
            String label9 = b9.getText();
            if (label9 != "X") {
                if (b5.getText().equals("")) {
                    b5.setText(label9);
                    b9.setText("");
                }
                if (b10.getText().equals("")) {
                    b10.setText(label9);
                    b9.setText("");
                }
                if (b13.getText().equals("")) {
                    b13.setText(label9);
                    b9.setText("");
                }
            }
        }//end of b9
        if (e.getSource() == b10) {
            String label10 = b10.getText();
            if (label10 != "X") {
                if (b6.getText().equals("")) {
                    b6.setText(label10);
                    b10.setText("");
                }
                if (b9.getText().equals("")) {
                    b9.setText(label10);
                    b10.setText("");
                }
                if (b11.getText().equals("")) {
                    b11.setText(label10);
                    b10.setText("");
                }
                if (b14.getText().equals("")) {
                    b14.setText(label10);
                    b10.setText("");
                }
            }
        }//end of b10
        if (e.getSource() == b11) {
            String label11 = b11.getText();
            if (label11 != "X") {
                if (b7.getText().equals("")) {
                    b7.setText(label11);
                    b11.setText("");
                }
                if (b10.getText().equals("")) {
                    b10.setText(label11);
                    b11.setText("");
                }
                if (b12.getText().equals("")) {
                    b12.setText(label11);
                    b11.setText("");
                }
                if (b15.getText().equals("")) {
                    b15.setText(label11);
                    b11.setText("");
                }
            }
        }//end of b11
        if (e.getSource() == b12) {
            String label12 = b12.getText();
            if (label12 != "X") {
                if (b8.getText().equals("")) {
                    b8.setText(label12);
                    b12.setText("");
                }
                if (b11.getText().equals("")) {
                    b11.setText(label12);
                    b12.setText("");
                }
                if (b16.getText().equals("")) {
                    b16.setText(label12);
                    b12.setText("");
                }
            }
        }//end of b12
        if (e.getSource() == b13) {
            String label13 = b13.getText();
            if (label13 != "X") {
                if (b9.getText().equals("")) {
                    b9.setText(label13);
                    b13.setText("");
                }
                if (b14.getText().equals("")) {
                    b14.setText(label13);
                    b13.setText("");
                }
            }
        }//end of b13
        if (e.getSource() == b14) {
            String label14 = b14.getText();
            if (label14 != "X") {
                if (b10.getText().equals("")) {
                    b10.setText(label14);
                    b14.setText("");
                }
                if (b13.getText().equals("")) {
                    b13.setText(label14);
                    b14.setText("");
                }
                if (b15.getText().equals("")) {
                    b15.setText(label14);
                    b14.setText("");
                }
            }
        }//end of b14
        if (e.getSource() == b15) {
            String label15 = b15.getText();
            if (label15 != "X") {
                if (b11.getText().equals("")) {
                    b11.setText(label15);
                    b15.setText("");
                }
                if (b14.getText().equals("")) {
                    b14.setText(label15);
                    b15.setText("");
                }
                if (b16.getText().equals("")) {
                    b16.setText(label15);
                    b15.setText("");
                }
            }
        }//end of b15
        if (e.getSource() == b16) {
            String label16 = b16.getText();
            if (label16 != "X") {
                if (b12.getText().equals("")) {
                    b12.setText(label16);
                    b16.setText("");
                }
                if (b15.getText().equals("")) {
                    b15.setText(label16);
                    b16.setText("");
                }
            }
        }//end of b15

        after = System.currentTimeMillis();
        long seconds = ((after - before) / 1000);
        lb2.setText("Time: " + seconds + " Seconds");

        if (e.getSource() == ref) {
            dispose();
            count = 0;
            before = 0;
            after = 0;
            new Puzzle16();
        }

        if (e.getSource() == res) {
            b1.setText(sample[0]);
            b2.setText(sample[1]);
            b3.setText(sample[2]);
            b4.setText(sample[3]);
            b5.setText(sample[4]);
            b6.setText(sample[5]);
            b7.setText(sample[6]);
            b8.setText(sample[7]);
            b9.setText(sample[8]);
            b10.setText(sample[9]);
            b11.setText(sample[10]);
            b12.setText(sample[11]);
            b13.setText(sample[12]);
            b14.setText(sample[13]);
            b15.setText(sample[14]);
            b16.setText(sample[15]);
        }
        if (b1.getText().equals(sample[0])
                && b2.getText().equals(sample[1])
                && b3.getText().equals(sample[2])
                && b4.getText().equals(sample[3])
                && b5.getText().equals(sample[4])
                && b6.getText().equals(sample[5])
                && b7.getText().equals(sample[6])
                && b8.getText().equals(sample[7])
                && b9.getText().equals(sample[8])
                && b10.getText().equals(sample[9])
                && b11.getText().equals(sample[10])
                && b12.getText().equals(sample[11])
                && b13.getText().equals(sample[12])
                && b14.getText().equals(sample[13])
                && b15.getText().equals(sample[14])
                && b16.getText().equals(sample[15])) {
            int a = JOptionPane.showConfirmDialog(this, "Congratulations, You won! Would you like new game?");
            if (a == JOptionPane.YES_OPTION) {
                dispose();
                count = 0;
                before = 0;
                after = 0;
                new Puzzle16();
            }
            if (a == JOptionPane.NO_OPTION) {
                dispose();
            }
        } else if (count == 100) {
            JOptionPane.showMessageDialog(this, "Too many click! Restart the game?");
            dispose();
            count = 0;
            before = 0;
            after = 0;
            new Puzzle16();
        }
    }

    public static void main(String[] args) {
        new Puzzle16();

    }
}