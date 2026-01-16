package lesson4;

import javax.swing.*;
import java.awt.*;

public class MyWindow extends JFrame {
    private JPanel panel;
    private JTextArea bigField;
    private JTextField smallField;
    private JButton sendButton;

    public MyWindow() throws HeadlessException {
        super("Простой чат");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setLocation(200,100);
        setSize(300,500);
        panel = new JPanel(new FlowLayout());
        JTextArea big, small;
        bigField = new JTextArea(20,20);
        smallField = new JTextField(20);
        sendButton = new JButton("Отправить");
        panel.add(bigField);
        panel.add(smallField);
        panel.add(sendButton);
        setContentPane(panel);
        setVisible(true);
    }

    public static void main(String[] args) {
        MyWindow window = new MyWindow();
    }
}