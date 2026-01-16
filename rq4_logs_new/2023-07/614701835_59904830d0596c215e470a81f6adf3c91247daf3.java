package employee.management.system;
import java.awt.*;
import javax.swing.*;
import java.awt.event.*;
import java.sql.*;

public class Login extends JFrame implements ActionListener{
    JTextField tfusername, tfpassword;
    Login(){
        getContentPane().setBackground(Color.WHITE);
        setLayout(null);
        
        JLabel lblusername = new JLabel("UserName");
        lblusername.setBounds(40,20,100,30);
        add(lblusername);
        
        tfusername = new JTextField();
        tfusername.setBounds(150,20,150,30);
        add(tfusername);
        
         JLabel lblpassword = new JLabel("Password");
        lblpassword.setBounds(40,70,100,30);
        add(lblpassword);
        
         tfpassword = new JTextField();
        tfpassword.setBounds(150,70,150,30);
        add(tfpassword);
        
        
        JButton Login = new JButton("LOGIN");
        Login.setBounds(150,140,150,30);
         Login.setBackground(Color.DARK_GRAY);
         Login.setForeground(Color.WHITE);
         Login.addActionListener(this);
         add(Login);
         
          
         ImageIcon i1= new ImageIcon(ClassLoader.getSystemResource("icons/second.jpg"));
         Image i2 = i1.getImage().getScaledInstance(200, 200, Image.SCALE_DEFAULT);
         ImageIcon i3 = new ImageIcon(i2);
         JLabel image = new JLabel(i3);
         image.setBounds(350, 0, 200, 200);
         add(image);
         
         
        setSize(600,300);
        setVisible(true);
        setLocation(450,200);
    }
    @Override
    public void actionPerformed(ActionEvent ae){
        try{
            String username = tfusername.getText();
            String password = tfpassword.getText();
            Conn c = new Conn();
            String query = ("select * from login where username = '"+username+"', password = '"+12345+"'");
            ResultSet rs = c.s.executeQuery(query);
            if(rs.next()){
                        setVisible(false);
                        }
            else{
            
            JOptionPane.showInternalMessageDialog(null, "INVALID USERNAME OR PASSWORD");
            setVisible(false);
            }
        }catch(Exception e){
        e.printStackTrace();
        }
    
    }
    public static void main(String args[]){
    new Login();
    }
    
}