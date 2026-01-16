
package chatting.app;
import java.awt.*;
import javax.swing.*; 
import java.awt.event.*;
import java.text.*;
import javax.swing.border.*;
import java.util.*;
import java.text.*;
import java.net.*;
import java.io.*;

public class Client  implements ActionListener
{
    JTextField text;
    static JPanel p2;
    static Box vertical=Box.createVerticalBox();
    static DataOutputStream dout;
    static JFrame f= new JFrame();
    Client()
    {
        f.setLayout(null);
        
        JPanel p1=new JPanel();
        p1.setBackground(new Color(7,94,84));
        p1.setBounds(0,0,500,70);
        f.add(p1);
        p1.setLayout(null);
        ImageIcon i1= new ImageIcon(ClassLoader.getSystemResource("icons/3.png"));
        Image i2 =i1.getImage().getScaledInstance(25, 25,Image.SCALE_DEFAULT);
        ImageIcon i3=new ImageIcon(i2); 
        JLabel back =new JLabel(i3);  
        back.setBounds(5,15,30,30);
        p1.add(back);
        back.addMouseListener(new MouseAdapter(){
            public void mouseClicked(MouseEvent ae){
                System.exit(0);
            }
        });
        ImageIcon i4= new ImageIcon(ClassLoader.getSystemResource("icons/bunty.jpeg"));
        Image i5 =i4.getImage().getScaledInstance(50, 50,Image.SCALE_DEFAULT);
        ImageIcon i6=new ImageIcon(i5); 
        JLabel profile =new JLabel(i6);
        profile.setBounds(45,10,50,50);
        p1.add(profile);
        
        ImageIcon i7= new ImageIcon(ClassLoader.getSystemResource("icons/video.png"));
        Image i8 =i7.getImage().getScaledInstance(35,35,Image.SCALE_DEFAULT);
        ImageIcon i9=new ImageIcon(i8); 
        JLabel video =new JLabel(i9);
        video.setBounds(315,15,35,35);
        p1.add(video);
        
        ImageIcon i10 = new ImageIcon(ClassLoader.getSystemResource("icons/phone.png"));
        Image i11 = i10.getImage().getScaledInstance(35, 35,Image.SCALE_DEFAULT);
        ImageIcon i12 =new ImageIcon(i11);
        JLabel phone = new JLabel(i12);
        phone.setBounds(380,15,35,35);
        p1.add(phone);
        phone.addMouseListener(new MouseAdapter(){
            public void mouseClicked(MouseEvent ae){
                System.out.println("Connecting...");
            }
        });
        
        ImageIcon i13=new ImageIcon(ClassLoader.getSystemResource("icons/3icon.png"));
        Image i14 = i13.getImage().getScaledInstance(20,30,Image.SCALE_DEFAULT);
        ImageIcon i15= new ImageIcon(i14);
        JLabel more=new JLabel(i15);
        more.setBounds(430,18,35,30);
        p1.add(more);
        
        JLabel name= new JLabel("BUNTY");
        name.setForeground(Color.WHITE);
        name.setFont(new Font("",Font.BOLD,25));
        name.setBounds(102,20,100,25);
        p1.add(name);
        
        JLabel status = new JLabel("online");
        status.setForeground(Color.WHITE);
        status.setFont(new Font("",Font.BOLD,15));
        status.setBounds(102,30,100,50);
        p1.add(status);
        
        p2 =new JPanel();
        p2.setBounds(0,75,500,510);
        f.add(p2);
        text = new JTextField();
        text.setFont(new Font ("SAN_SERIF", Font.PLAIN,16));
        text.setBounds(5,590,320,40);
        f.add(text);
        
        JButton button =new JButton("Send");
        button.setFont(new Font ("SAN_SERIF", Font.BOLD,20));
        button.setForeground(Color.WHITE);
        
        button.setBackground(new Color(7,94,84));
        button.setBounds(340,590,120,40);
        button.addActionListener(this);
        f.add(button);
        
        f.setSize(523,700);
        f.setLocation(800,200);
        f.getContentPane().setBackground(Color.WHITE);
      
        f.setVisible(true);
    }
    public void actionPerformed(ActionEvent ae){
        try{
       String message=text.getText();
       JPanel p3 =format(message);
       
       p2.setLayout(new BorderLayout());
       
       JPanel right =new JPanel(new BorderLayout());
       right.add(p3,BorderLayout.LINE_END);
       vertical.add(right);
       vertical.add(Box.createVerticalStrut(15));
       p2.add(vertical,BorderLayout.PAGE_START);
      
       
       dout.writeUTF(message);
        
       text.setText("");
       f.repaint();
       f.invalidate();
       f.validate();
       }catch(Exception e)
        {
            e.printStackTrace();
        }
       
       
    }
    public static JPanel format (String message){
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel,BoxLayout.Y_AXIS));
        JLabel output = new JLabel("<html><p style=\"width:150px\">"+ message+ "</p></html>");
        output.setFont(new Font("Tahoma",Font.PLAIN,18));
        output.setForeground(Color.WHITE);
        output.setBackground(new Color(9,147,122));
        output.setBorder(new EmptyBorder(5,15,5,15));
        output.setOpaque(true);
        panel.add(output);
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sjf = new SimpleDateFormat("HH:mm");
       JLabel time = new JLabel();
       time.setText(sjf.format(cal.getTime()));
       panel.add(time);
       
       
        return panel;
        
    }
    public static void main(String[] args){
        new Client();
        
        try{
           Socket skt = new Socket("127.0.0.1",8000);
            while(true){
               
                DataInputStream din =new DataInputStream(skt.getInputStream());
                dout =new DataOutputStream(skt.getOutputStream());
                while(true)
                {
                    p2.setLayout(new BorderLayout());
                    String msg= din.readUTF();
                    JPanel panel= format(msg);
                    JPanel left =new JPanel(new BorderLayout());
                    left.add(panel,BorderLayout.LINE_START);
                    vertical.add(left);
                    vertical.add(Box.createVerticalStrut(15));
                    p2.add(vertical,BorderLayout.PAGE_START);
                    
                    
                    f.validate();
                    
                }
            }
        } catch(Exception e){
          e.printStackTrace();  
        }
        
    }
    
}