import java.io.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;



public class Login extends JFrame {
   
   private JTextField userText;
   private JTextArea notifications;
   private JButton submit;
   private int HEIGHT;
   private int LENGTH;
   private String userName;
   
   public Login(int height, int length) {
      super("User - Login");
      userName = "";
      HEIGHT = height;
      LENGTH = length;
      userText = new JTextField();
      userText.setEditable(true);
      userText.addActionListener(
         new ActionListener() {
            public void actionPerformed(ActionEvent event) {
               String test = event.getActionCommand();
               if(test.trim().equals("")) {
                  showMessage("\nLogin is invalid, please enter something!");   
               }
               else {
                  userName = test;
               }
               userText.setText("");
            }
         }
      );
      add(userText, BorderLayout.NORTH);
      notifications = new JTextArea();
      add(new JScrollPane(notifications));
      setSize(HEIGHT, LENGTH); //size of window
      setVisible(true);
      notifications.setFont(new Font("Serif", Font.PLAIN, 30));
      userText.setFont(new Font("Serif", Font.PLAIN, 30));   
   }
   
   public String getUserName() {
      return this.userName;
   }
   
   //closes the login window
   public void close() {
      System.exit(0);
   }
      
   //shows messages on the JFrame
   private void showMessage(final String text) {
   	//setting aside a thread to update a gui, so we dont have to create an entire new gui when we just want to update part
      SwingUtilities.invokeLater(
         new Runnable() {
            public void run() {
               notifications.append(text); //adds message to the chatWindow
            }
         }
         );
   }

   
   
}