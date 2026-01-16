package ru.sandem.java3.lesson4;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

public class MainWindow extends JFrame {

    private JTextField messageTextField;
    private JTextArea chatTextArea;
    private JButton sendMessageButton;

    public MainWindow() {

        setTitle("Chat v1.0");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setBounds(400, 250, 650, 400);

        chatTextArea = new JTextArea();
        chatTextArea.setEditable(false);
        chatTextArea.setLineWrap(true);

        JScrollPane scrollPane = new JScrollPane(chatTextArea);
        add(scrollPane, BorderLayout.CENTER);

        JPanel bottomPanel = new JPanel(new BorderLayout());
        add(bottomPanel, BorderLayout.SOUTH);

        messageTextField = new JTextField("Сообщение");
        bottomPanel.add(messageTextField, BorderLayout.CENTER);

        sendMessageButton = new JButton("Отправить");
        bottomPanel.add(sendMessageButton, BorderLayout.EAST);

        messageTextField.addMouseListener(new MouseListener() {
            @Override
            public void mouseClicked(MouseEvent e) {
                messageTextField.setText("");
            }

            @Override
            public void mousePressed(MouseEvent e) {

            }

            @Override
            public void mouseReleased(MouseEvent e) {

            }

            @Override
            public void mouseEntered(MouseEvent e) {

            }

            @Override
            public void mouseExited(MouseEvent e) {

            }
        });
        messageTextField.addActionListener(e -> sendMessageToChat());
        sendMessageButton.addActionListener(e -> sendMessageToChat());
        setVisible(true);
    }

    private void sendMessageToChat(){
        chatTextArea.setText(chatTextArea.getText() + messageTextField.getText() + "\n");
        messageTextField.setText("");
    }
}