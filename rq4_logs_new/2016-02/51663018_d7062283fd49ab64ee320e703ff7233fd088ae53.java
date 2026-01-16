import java.io.*;
import java.net.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;





public class Client extends JFrame {

	private JTextField userText;
	private JTextArea chatWindow;
	private ObjectOutputStream output;
	private ObjectInputStream input;
	private String message = "";
	private String serverIP;
	private Socket connection;
	private final int HEIGHT = 1000;
	private final int LENGTH = 1000;
	private int port = 6789;

	//constructor 
	public Client(String host) {
		super("Instant Messenger");
		serverIP = host;
		userText = new JTextField();
		userText.setEditable(false);
		userText.addActionListener(
			new ActionListener() {
				public void actionPerformed(ActionEvent event) {
					sendMessage(event.getActionCommand());
					userText.setText("");
				}
			}
		);
		add(userText, BorderLayout.NORTH);
		chatWindow = new JTextArea();
		add(new JScrollPane(chatWindow), BorderLayout.CENTER);
		setSize(HEIGHT, LENGTH);
		setVisible(true);
		chatWindow.setFont(new Font("Serif", Font.PLAIN, 30));
		userText.setFont(new Font("Serif", Font.PLAIN, 30));
	}

	//connect to server 
	public void startRunning() {
		try {
			connectToServer();
			setupStreams();
			whileChatting();
		}
		catch(EOFException e) {
			showMessage("\n Client terminated connection");

		}
		catch(IOException e2) {
			e2.printStackTrace();
		}
		finally {
			closeStuff();
		}
	}

	//connect to a server
	private void connectToServer() throws IOException {
		showMessage("Attempting connection... \n");
		connection = new Socket(InetAddress.getByName(serverIP), port);
		showMessage("Connected to: " + connection.getInetAddress().getHostName());

	}

	//setting up streams for the client 
	private void setupStreams() throws IOException {
		output = new ObjectOutputStream(connection.getOutputStream());
		output.flush();
		input = new ObjectInputStream(connection.getInputStream());
		showMessage("\n The streams are set up and good to go! \n");
	}

	//while chatting with server 
	private void whileChatting() throws IOException {
		ableToType(true);
		do {
			try {
				message = (String) input.readObject();
				showMessage("\n" + message);
			}
			catch(ClassNotFoundException e) {
				showMessage("\n I don't know that object type!");
			}
		}
		while(!message.equals("SERVER - END"));
	}

	//house keeping, closing all the streams and sockets down
	private void closeStuff() {
		showMessage("\n Closing stuff down");
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

	//send messages to server 
	private void sendMessage(String message) {
		try {
			output.writeObject("CLIENT - " + message); //sends the message to the server
			output.flush();
			showMessage("\nCLIENT - " + message); //shows message in the users chat window
		}
		catch(IOException e) {
			chatWindow.append("\n something messed up sending message");
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