package com.company;

import javax.swing.*;
import javax.swing.border.LineBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class MyWindow extends JFrame {
            public MyWindow() {
                setBounds(400, 200, 300, 300);
                setTitle("ЧАТ");
                setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
                JTextArea textArea = new JTextArea();
                textArea.setLineWrap(true);
                JScrollPane jsp = new JScrollPane(textArea);
                add(jsp, BorderLayout.CENTER);
                JPanel south = new JPanel(new BorderLayout());
                add(south, BorderLayout.SOUTH);
                JButton enter = new JButton("ENTER");
                south.add(enter, BorderLayout.EAST);
                JTextField textField = new JTextField();
                south.add(textField, BorderLayout.CENTER);
                enter.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        System.out.println("Clicked");
                    }

                });

                setVisible(true);
            }
        }





















