import java.awt.*;
import javax.swing.*;

public class Test {
    
    public static void Page(){
        //homepage
        JFrame      screen = new JFrame("Home Page");
        JPanel      jPanel = new JPanel();
        
        screen.setTitle("OXGame");
        screen.setVisible(true);
        screen.setSize(1024,780);
        screen.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        screen.validate();
        jPanel.setBackground(Color.yellow);
        jPanel.add(new JButton("START"));
        jPanel.add(new JButton("HOW TO"));
        
        ImageIcon img = new ImageIcon("‪c:\\Tic-Tag-Toe.png");   
       
        JLabel label3 = new JLabel("S",img,JLabel.CENTER);
        screen.add(jPanel);
        
    }

    public static void main(String args[]){
        Page();
        
    }
}