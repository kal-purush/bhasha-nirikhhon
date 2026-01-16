import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;

public class Chat{
    public JTextField textField;
    public JTextArea textArea;
    public Chat (){       
        
        JFrame frame = new JFrame("Chat");
        frame.setSize(400,400);        
        
        JPanel p1 = new JPanel();
        p1.setLayout(new BorderLayout());
            
        JPanel p2 = new JPanel();
        p2.setLayout(new BorderLayout());        
        
        textField = new JTextField();
        p1.add(textField, BorderLayout.CENTER);
        
        JButton b1 = new JButton("Send");
        p1.add(b1, BorderLayout.EAST); 
        
        textArea = new JTextArea();
        p2.add(textArea, BorderLayout.CENTER);
        p2.add(p1, BorderLayout.SOUTH);
        
        frame.setContentPane(p2);
        frame.setVisible(true);                        
    }
}