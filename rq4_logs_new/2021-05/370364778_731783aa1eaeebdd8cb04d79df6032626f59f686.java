package ru.geekbrains.lesson8;

import javax.swing.*;
import javax.swing.border.Border;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class MyWindow extends JFrame {
    private JTextField textField;
    private int randomNumber;


    public MyWindow(){

        this.randomNumber = (int)(Math.random() * 10) + 1;

        setTitle("My Window");
        setBounds(400, 300, 600, 140);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        //setResizable(false);

        textField = new JTextField();
        add(textField, BorderLayout.NORTH);

        textField.setText("Программа загадала число от 1 до 10");
        textField.setEnabled(false);
        textField.setHorizontalAlignment(SwingConstants.CENTER);

        Font font = new Font("Ariel", Font.PLAIN, 18);
        textField.setFont(font);

        JButton button1 = new JButton("Restart");
        add(button1, BorderLayout.SOUTH);
        button1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
               
                randomNumber = (int)(Math.random() * 10) + 1;

            }
        });

        JPanel buttonsPanel = new JPanel(new GridLayout(1,10));
        add(buttonsPanel, BorderLayout.CENTER);

        for (int i = 1; i <= 10 ; i++) {
            JButton button = new JButton(String.valueOf(i));
            button.setFont(font);
            buttonsPanel.add(button);

            int buttonIndex = i;
            button.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    tryToAnswer(buttonIndex);
                    }

            });

        }
        setVisible(true);


    }
    public void tryToAnswer(int answer) {
            if (answer == randomNumber) {
                textField.setText("Победил!!! Число: " + randomNumber);
            } else if (answer > randomNumber) {
                textField.setText("Не угадал! Загаданное число меньше!");
            } else {
                textField.setText("Не угадал! Загаданное число больше!");
            }
        }





}