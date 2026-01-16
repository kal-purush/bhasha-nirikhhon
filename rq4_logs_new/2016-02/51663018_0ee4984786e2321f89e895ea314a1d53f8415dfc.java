import java.io.*;
import java.net.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;




public class Server extends JFrame {

	private JTextField userText;
	private JTextArea chatWindow;
	private ObjectOutputStream output;
	private ObjectInputStream input;
	private ServerSocket server;
	private Socket connection;
	private final int HEIGHT = 1000;
	private final int LENGTH = 1000;
	private final int port = 6789;

	//constructor
	public Server() {
		super("Instant Messenger");
		userText = new JTextField();
		userText.setEditable(false); //before you are connected you cannot type anything in message box 
		userText.addActionListener(
			new ActionListener() {
				public void actionPerformed(ActionEvent event) {
					sendMessage(event.getActionCommand());
					userText.setText(""); //turns message box blank after message is sent

				}
			}
		);
		add(userText, BorderLayout.NORTH);
		chatWindow = new JTextArea();
		add(new JScrollPane(chatWindow));
		setSize(HEIGHT, LENGTH); //size of window
		setVisible(true);
		chatWindow.setFont(new Font("Serif", Font.PLAIN, 30));
		userText.setFont(new Font("Serif", Font.PLAIN, 30));
	}


	//set up and run the server 
	public void startRunning() {
		try {
			server = new ServerSocket(port, 100); //port #, backlog(how many people can wait to access this instant messenger)
			while(true) {
				try {
					//connect and have conversation
					waitForConnection();
					setupStreams();
					whileChatting();
				}
				catch(EOFException ex) {
					showMessage("\n Server ended the connection!");
				}
				finally {
					closeStuff();
				}
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

	//wait for connection, then once connected give prompt to user
	private void waitForConnection() throws IOException {
		showMessage(" Waiting for someone to connect... \n");
		connection = server.accept(); //once someone asks to connect this accepts the connection to the socket, once connected a connection is created bewteen server and client 
		showMessage(" Now connected to "+connection.getInetAddress().getHostName());

	}

	//get stream to send and receive data
	private void setupStreams() throws IOException {
		output = new ObjectOutputStream(connection.getOutputStream()); //creates pathway that allows to connect to another computer
		output.flush(); // clears the stream so there is no left over data
		input = new ObjectInputStream(connection.getInputStream()); //sets pathway to receive messages 
		showMessage("\n Streams are now setup! \n");
	}

	//during the chat conversation 
	private void whileChatting() throws IOException {
		String message = "You are now connected! ";
		sendMessage(message);
		ableToType(true);
		do {
			//have conversation
			try {
				message = (String) input.readObject();
				//stuff here to unencrypted the message that was received
				showMessage("\n"+message);
			}
			catch(ClassNotFoundException e) {
				showMessage("\n idk wtf that user sent!");
			}
		}
		while(!message.equals("CLIENT - END")); //if the client wants end then use this string 
	}

	//close streams and sockets after you are done chating 
	private void closeStuff() {
		showMessage("\n Closing connections... \n");
		ableToType(false);
		try {
			output.close();
			input.close();
			connection.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

	//send a message to the client 
	private void sendMessage(String message) {
		try {
			//stuff here to encrypt the message before it is sent
			output.writeObject("SERVER - " + message);
			output.flush();
			showMessage("\nSERVER - "  + message);
		}
		catch(IOException e) {
			chatWindow.append("\n ERROR: DUDE I CANT SEND THAT MESSAGE");
		}
	}

	//updates chatWindow
	private void showMessage(final String text) {
		//setting aside a thread to update a gui, so we dont have to create an entire new gui when we just want to update part
		SwingUtilities.invokeLater(
			new Runnable() {
				public void run() {
					chatWindow.append(text); //adds message to the chatWindow
				}
			}
		);
	}

	//lets the user type stuff into there chat box
	private void ableToType(final boolean trueOrFalse) {
		SwingUtilities.invokeLater(
			new Runnable() {
				public void run() {
					userText.setEditable(trueOrFalse);
				}
			}
		);
	} 
}