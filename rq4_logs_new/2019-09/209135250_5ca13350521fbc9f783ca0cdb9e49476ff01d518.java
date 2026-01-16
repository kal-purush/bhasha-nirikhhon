/**
 * Title: Calculator Midterm
 * Programmer: Werner Ordonez
 * Date: 6/26/2017
 * Details: This program displays a calculator where a user can do operations
 * like /,*,+,-,^,S,%
 */
package calculator;

/**
 *
 * @author wernerordonez
 */
//importing necesary classes
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

class CalculatorPanel extends JPanel
        implements ActionListener {

    //set up GUI calc panel
    public CalculatorPanel() {
        setLayout(new BorderLayout());
        //diplay for user input
        display = new JLabel("0", SwingConstants.RIGHT);
        display.setFont(new Font("Serif", Font.BOLD, 36));
        add(display, "North");
        display.setForeground(Color.blue);
        //create JPanel
        JPanel p = new JPanel();
        p.setLayout(new GridLayout(5, 4, 4, 4));
        p.setBackground(Color.red);
        //String for all the buttons
        String buttons = "789/456*123-0.=+^S%C"; //string of all the buttons 
        //create all buttons in JPanel
        for (int i = 0; i < buttons.length(); i++) {
            addButton(p, buttons.substring(i, i + 1));
        }
        add(p, "Center");
    }

    //add buttons to the jpanel
    private void addButton(Container c, String s) {
        JButton b = new JButton(s);

        b.setBackground(Color.yellow); // set the color of the button yellow
        b.setForeground(Color.black);
        b.setFont(new Font("Courier", Font.BOLD, 36));
        b.addActionListener(this);
        c.add(b);

    }

    //user interface event handeler
    public void actionPerformed(ActionEvent evt) {
        try {
            String s = evt.getActionCommand();
            // if user presses C set interface to 0 
            if (s.equals("C")) {

                display.setForeground(Color.blue);
                display.setText("0");
                display.setToolTipText("The result is 0");

            } else {
                if ('0' <= s.charAt(0) && s.charAt(0) <= '9'
                        || s.equals(".")) {
                    if (start) {
                        display.setText(s);
                    }

                    display.setText(display.getText() + s);
                    start = false;
                } else if (start) {
                    if (s.equals("-")) {
                        display.setText(s);
                    } else {
                        op = s;
                    }

                } else {
                    double x = Double.parseDouble(display.getText());
                    calculate(x);
                    op = s;
                    start = true;
                }
            }

        } catch (Exception e) {
            JOptionPane.showMessageDialog(null, "Invalid Operation!");
            start = true;
            if (start) {
                display.setText("0");
            }

        }
    }

    public void calculate(double n) {

        switch (op) {
            case "+":
                arg += n;
                break;
            case "-":
                arg -= n;
                break;
            case "*":
                arg *= n;
                break;
            case "/":
                arg /= n;
                break;
            case "=":
                arg = n;
                break;
            case "^":
                arg = Math.pow(arg, n);
                break;
            case "S":
                arg = Math.sqrt(n);
                break;
            case "%":
                arg *= n / 100;
                break;
            case "C":
                arg = 0;
                break;
            default:
                break;
        }

        if (arg < 0) {
            display.setForeground(Color.red);
        } else {
            display.setForeground(Color.blue);
        }

        display.setText("" + arg);
        display.setToolTipText("The result is " + arg);

    }

    private final JLabel display;
    private double arg = 0;
    private String op = "=";
    private boolean start = true;

}

class CalculatorFrame extends JFrame {

    public CalculatorFrame() {
        setTitle("Calculator");
        setSize(400, 400);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(0);
            }
        });
        Container contentPane = getContentPane();
        contentPane.add(new CalculatorPanel());
    }
}

public class Calculator {

    public static void main(String[] args) {
        JFrame frame = new CalculatorFrame();
        frame.setVisible(true);
    }

}