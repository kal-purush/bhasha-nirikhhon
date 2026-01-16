package networkChat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {
	Socket csock;
	Server server;
	BufferedReader in;
	PrintWriter out;
	
	public ClientHandler(Socket client, Server server) {
		this.csock = client;
		this.server = server;
		boot();
	}
	
	public void boot() {
		// set up IO
		try {
			out = new PrintWriter(csock.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(csock.getInputStream()));
		} catch (IOException e) {
            exitWith("Exception caught when trying to set up IO with a client", 1);
		}
	}
	
	/**
	 * Print an error to stdout and exit.
	 * 
	 * @param desc		String to print before exiting.
	 * @param exit_code	Integer to exit with.
	 */
	private void exitWith(String desc, int exit_code) {
		System.err.println("ERROR: " + desc);
		System.exit(exit_code);
	}
	
	public void run() {
		while (true) {
			// read client messages
			String input;
			try {
				while ((input = in.readLine()) != null) {
					server.relayMessage(input);
					//System.out.println("I got a message!: " + input);
				}
			} catch (IOException e) {
				exitWith("Exception caught when trying to read from client", 1);
			}
		}
	}

	//public void sendMessage(String[] message) {
	public void sendMessage(String message) {
		out.println(message);
	}
}