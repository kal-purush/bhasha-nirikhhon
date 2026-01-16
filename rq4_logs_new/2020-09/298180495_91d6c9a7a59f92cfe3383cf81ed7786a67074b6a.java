package jp.co.zzz.userInterface;

import javax.swing.JFrame;
import javax.swing.JButton;
import java.awt.Container;
import java.awt.BorderLayout;

class GuiMain{
    public static void main(String args[]){
        JFrame frame = new JFrame("MyTitle");
        frame.setBounds(100, 100, 600, 400);

        JButton button = new JButton("Push");

        frame.getContentPane().add(button, BorderLayout.WEST);

        //button.addActionListener(this);

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }
}