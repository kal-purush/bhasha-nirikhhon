package appgui;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import chatapp.networking.MessageListener;
import chatapp.networking.MessageTransmitter;
import chatapp.networking.WritableGUI;

import javax.swing.JTextField;
import javax.swing.JButton;
import javax.swing.JLabel;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JTextArea;

public class MainScreen extends JFrame implements WritableGUI{

	//ports 8877 8878 8878 8877 listen -> send -> listen
	//Idk why it's not working properly.
	private JPanel contentPane;
	private JTextField ipTextField;
	private JTextField targetPort;
	private JTextField messageText;
	private JTextField receivePort;
	private JTextArea chat;      

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MainScreen frame = new MainScreen();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public MainScreen() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 591, 401);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		ipTextField = new JTextField();
		ipTextField.setBounds(242, 24, 212, 28);
		contentPane.add(ipTextField);
		ipTextField.setColumns(10);
		
		targetPort = new JTextField();
		targetPort.setBounds(466, 24, 97, 28);
		contentPane.add(targetPort);
		targetPort.setColumns(10);
		
		JLabel lblIpAddress = new JLabel("IP Address");
		lblIpAddress.setBounds(244, 6, 97, 16);
		contentPane.add(lblIpAddress);
		
		JLabel lblPortNumber = new JLabel("Port Number");
		lblPortNumber.setBounds(468, 6, 89, 16);
		contentPane.add(lblPortNumber);
		
		JTextArea chat = new JTextArea();
		chat.setBounds(12, 59, 551, 247);
		contentPane.add(chat);
		
		messageText = new JTextField();
		messageText.setBounds(12, 318, 412, 28);
		contentPane.add(messageText);
		messageText.setColumns(10);
		
		JButton sendButton = new JButton("Send");
		sendButton.setBounds(436, 318, 114, 28);
		contentPane.add(sendButton);
		
		JButton listenButton = new JButton("Listen");
		listenButton.setBounds(12, 24, 90, 28);
		contentPane.add(listenButton);
		
		receivePort = new JTextField();
		receivePort.setBounds(108, 24, 122, 28);
		contentPane.add(receivePort);
		receivePort.setColumns(10);
	}
	
	MessageListener listener;
	//add event to button
	private void listenButtonActionPerformd(java.awt.event.ActionEvent evt) {
		listener = new MessageListener(this, Integer.parseInt(receivePort.getText()));//initialize listener gui-this, port - parce from string to int
		listener.start();
	}
	
	private void sendButtonActionPerformd(java.awt.event.ActionEvent evt) {
		MessageTransmitter transmitter = new MessageTransmitter(messageText.getText(), ipTextField.getText(), Integer.parseInt(targetPort.getText()));
		transmitter.start();
	}

	@Override 
	public void write(String s) { //our interface		
		chat.append (s+System.lineSeparator());		
	}
}