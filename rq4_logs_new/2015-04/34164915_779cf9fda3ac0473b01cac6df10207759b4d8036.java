import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

/**
 * 
 * Elohe Yonas
 *CSC201
 */
public class Atm extends JFrame {
	/**
	 * first, initializing variables for the GUI
	 */
	private JPanel keypad;
	/**
	 * to minimize lines, i used for loop for the numberpads
	 */
	protected JButton[] keybutton = new JButton[10];
	private JButton clear;
	private JButton enter;
	private JPasswordField password;
	private String inputkey = "";

Atm(){

password = new JPasswordField();
password.setLayout(new BorderLayout());
/**
 * time to make the clear button clear the text of JPasswordField
 */
clear = new JButton("clear");
clear.addActionListener(new ActionListener(){
	public void actionPerformed(ActionEvent e){
		inputkey = "";
		password.setText("");
	}
});

/**
 * now for the enter button to gain an action
 */
enter = new JButton("enter");

enter.addActionListener(new ActionListener(){
	public void actionPerformed(ActionEvent e){
		
		String input = password.getText();
		int count = 0;
		
		if(Authentication.authenticatekey(inputkey)){
			JOptionPane.showMessageDialog(null, "you got it man","success!",1);
		System.out.println(inputkey);
		inputkey = "";
		password.setText("");
		}
		else if(count<3){
			JOptionPane.showMessageDialog(null,"well...that's strike" + (count + 1) +", 3 and you're done","ouch...",1);
		System.out.println("no");
		inputkey = "";
		password.setText("");
		count++;
		
}
		else{
			JOptionPane.showMessageDialog(null,"you might wanna call an Admin at this point...", "huh....",1);
			System.exit(0);
		}
	
	}
});

/**
 * setting layout for keypad
 */
keypad = new JPanel();	
keypad.setLayout(new BorderLayout());
keypad.setLayout(new GridLayout(4,3));
add(password, BorderLayout.NORTH);
for(int i=0; i<9;i++){
 keybutton[i] = new JButton(String.valueOf(i+1));
 keybutton[i].addActionListener(new ActionListener(){
	 public void actionPerformed(ActionEvent e){
		 
		 inputkey += e.getActionCommand();
		 password.setText(inputkey);
	
	 }
 });
}
keybutton[9] = new JButton(String.valueOf(0));
keybutton[9].addActionListener(new ActionListener(){
	 public void actionPerformed(ActionEvent e){
		 
		 inputkey += e.getActionCommand();
		 password.setText(inputkey);
	
	 }
 });
/**
 * adding buttons
 */
add(keypad, BorderLayout.WEST);
for(int i=0; i<9;i++){
	keypad.add(keybutton[i]);
	}
keypad.add(keybutton[9]);
keypad.add(clear);
keypad.add(enter);
setTitle("ATM PIN AUTHENTIFICATION");
}
}
