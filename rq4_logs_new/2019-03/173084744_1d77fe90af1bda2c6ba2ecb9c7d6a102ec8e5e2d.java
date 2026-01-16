package lesson2.gui.elements_for_changing_name;

import lesson2.dbconn.JdbcExample;
import lesson2.gui.Network;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class DialogWindow extends JFrame {
    private final Network network;

    public DialogWindow(Network network) {
        super("Введите новое имени пользователя");
        this.network = network;
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        JTextField tf = new JTextField("Введите новое имя пользователя");

        JButton bOK = new JButton("OK");
        bOK.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String currentUserName = network.getUsername();
                String newUserName = tf.getText();
                JdbcExample.changeNick(currentUserName, newUserName);
                dispose();
            }
        });

        JButton bCancel = new JButton("Отмена");
        bCancel.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                dispose();
            }
        });

        JPanel textPanel = new JPanel();
        JPanel buttonPanel = new JPanel();
        buttonPanel.add(bCancel);
        buttonPanel.add(bOK);

        textPanel.add(tf);

        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new GridLayout(2, 1, 5, 10));
        mainPanel.add(textPanel);
        mainPanel.add(buttonPanel);
        getContentPane().add(mainPanel);
        pack();
        setVisible(true);
    }

    /*public static void main(String[] args) {
        DialogWindow dialogWindow = new DialogWindow();
    }*/
}