

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;

public class paintit1 extends JFrame implements ActionListener {
        Color c = Color.BLACK;
        JFrame frame;
        JButton button, button1, button2, button3, erase,rubber;
        JPanel j;
        Graphics2D g;
        MyDrawPanel DrawPanel;
        int m=5;
        int n=5;
public void Male() {
        frame = new JFrame();
        frame.getContentPane().setBackground(Color.WHITE);
        button = new JButton("CLEAR");
        button.setForeground(Color.black);
        button.setBackground(Color.orange);
        button.setFocusPainted(false);
        button.setBorder(BorderFactory.createLineBorder(Color.RED));

        j = new JPanel();
        button1 = new JButton("RED");
        button2 = new JButton("yellow");
        button3 = new JButton("Green");
        erase = new JButton("blue");
        rubber = new JButton("erase");
        button1.setFocusPainted(false);
        button2.setFocusPainted(false);
        button3.setFocusPainted(false);
        erase.setFocusPainted(false);
        button.addActionListener(this);
        rubber.addActionListener(this);
        DrawPanel = new MyDrawPanel();
        frame.setBackground(Color.white);
        frame.add(BorderLayout.WEST, button);
        j.setBorder(BorderFactory.createLineBorder(Color.RED));
        j.add(button1);
        button1.setForeground(Color.black);
        button1.setBackground(Color.RED);
        rubber.setForeground(Color.black);
        rubber.setBackground(Color.WHITE);

        j.add(button2);
        button2.setForeground(Color.BLACK);
        button2.setBackground(Color.yellow);
        j.add(rubber);
        j.add(button3);
        button3.setForeground(Color.BLACK);
        button3.setBackground(Color.GREEN);
        j.add(erase);
        erase.setForeground(Color.BLACK);
        erase.setBackground(Color.BLUE);
        j.setBackground(Color.ORANGE);
        j.setLayout(new FlowLayout());
        frame.add(BorderLayout.SOUTH, j);
        button1.addActionListener(this);
        button2.addActionListener(this);
        button3.addActionListener(this);
        erase.addActionListener(this);
        frame.add(BorderLayout.CENTER, DrawPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(500, 500);
        frame.setVisible(true);
        }
    public void actionPerformed(ActionEvent event) {
        if (event.getSource() == button) {
            frame.repaint();
        }
        if (event.getSource() == button1) {
            c = Color.RED;
            m=10;
            n=5;
        }
        if (event.getSource() == button2) {
            c = Color.YELLOW;
            m=5;
            n=5;
        }
        if (event.getSource() == button3) {
            c = Color.GREEN;
            m=5;
            n=5;
        }
        if (event.getSource() == erase) {
            c = Color.BLUE;
            m=5;
            n=5;
        }
        if (event.getSource() == rubber) {
            c = Color.WHITE;
            m=10;
            n=10;
        }
    }

    public static void main(String[] args) {
       // SwingUtilities.invokeLater(new Runnable() {
            //public void run() {
                paintit1 gui = new paintit1();
                gui.Male();
           // }
       // });

    }

    class MyDrawPanel extends JPanel implements MouseMotionListener {


        public void paintComponent(Graphics g) {
            addMouseMotionListener(this);

        }

        @Override
        public void mouseDragged(MouseEvent e) {
            g = (Graphics2D) getGraphics();
            g.setColor(c);
            g.fillOval(e.getX(), e.getY(),m, n);
        }

        public void mouseMoved(MouseEvent e) {
//empty
        }
    }
}

