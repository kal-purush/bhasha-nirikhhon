package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson4.hw4;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class CreateWindowChat extends JFrame{
    public JTextArea textArea;
    public CreateWindowChat(){
        this.setTitle("Chat");
        this.setSize(new Dimension(500,500));
        this.getContentPane().setLayout(new BorderLayout());
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.add(downMenu(),BorderLayout.SOUTH);
        this.add(getMessage(),BorderLayout.CENTER);
        this.setResizable(false);
        this.setVisible(true);
    }
    private JPanel downMenu(){
        JPanel downMenu = new JPanel();
        downMenu.setBackground(Color.lightGray);
        downMenu.setLayout(new FlowLayout());
        downMenu.setSize(500,100);
        JTextField textField = new JTextField();
        textField.setPreferredSize(new Dimension(300,30));
        textField.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                textArea.append(textField.getText()+"\n");
                textField.setText("");
            }
        });
        downMenu.add(textField);
        JButton button = new JButton("Отправить");
        button.setPreferredSize(new Dimension(100,30));
        button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String newMessage = textField.getText();
                textArea.append(newMessage+"\n");
                textField.setText("");
            }
        });
        downMenu.add(button);
        downMenu.setVisible(true);
        return downMenu;
    }
    private JPanel getMessage(){
        JPanel getMessage = new JPanel();
        getMessage.setLayout(new FlowLayout());
        getMessage.setBackground(Color.gray);
        getMessage.setPreferredSize(new Dimension(500,300));
        getMessage.setBorder(BorderFactory.createLineBorder(Color.black, 4));
        textArea = new JTextArea();
        textArea.setPreferredSize(new Dimension(420,350));
        textArea.setEditable(false);//запрет на изменение текста
        JScrollPane scrollBar = new JScrollPane(textArea,JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        scrollBar.setPreferredSize(new Dimension(20,300));
        getMessage.add(textArea);
        getMessage.add(scrollBar);
        getMessage.setVisible(true);
        return getMessage;
    }
}