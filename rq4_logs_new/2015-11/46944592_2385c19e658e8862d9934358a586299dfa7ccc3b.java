package cse.uta.edu.os2.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * this class responsible for creating the client program socket and make 
 * a request to the server for synonym for a word 
 * @author lakshmana s(1000789751)
 *
 */
public class ClientProg {
	
	private Socket socket=null;
	private BufferedReader is=null;
	private PrintWriter os=null;
	
	/** 
	 * creates the client socket and with the given server port 
	 * and creates the input and output stream for that opened socket
	 */
	public ClientProg()
	{
		try {
			socket = new Socket("127.0.0.1",1234);
			is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			os = new PrintWriter(socket.getOutputStream(),true);
		}
		catch (UnknownHostException e) {
			System.out.println("client : unknown host ");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("client : Error connecting to server socket,make sure to start the Server before starting Client ");
			e.printStackTrace();
			System.exit(-1);
		}
		 	
	}
	
	/**
	 * in this method client requests a server with the word and 
	 * waits for the server to respond and displays the server response
	 */
	public void readWriteScoket(){
		String line=null;
		String text="Hi, there, How u doing today ?";
		System.out.println("client :");
		BufferedReader sysIn = new BufferedReader(new InputStreamReader(System.in));
		try {
			while((text = sysIn.readLine())!=null){
				//writes it to the client output stream 
				os.println(text);
				//reads from the server output stream
				line =is.readLine();
				System.out.println("server : "+line);
				System.out.println("client : ");
			}
		} catch (IOException e) {
			System.out.println("client : Reading from socket failed");
			e.printStackTrace();
		}
	}
	
	/**
	 * client sends out a message to the server
	 * @param msg
	 */
	public void sendMessage(String msg){
		if(msg!=null){
			os.println(msg);
		}
		
	}

	/**
	 * client waits for the server to write to it's output stream
	 * and reads them
	 * @return
	 */
	public String recieveMessage(){
		String msg=null;
		try {
			msg =is.readLine();
		} catch (IOException e) {
			System.out.println(this.getClass().getName()+" Exception while recieving data from server");
			e.printStackTrace();
		}
		return msg;
		}
	
	/**
	 * this method closes the client socket and input and output stream
	 */
	public void closeSocket(){
		try {
			is.close();
			os.close();
			socket.close();
		} catch (IOException e) {
			System.out.println("client : Error while closing the socket ");
			e.printStackTrace();
		}
	}
	
	/**
	 * main method to test the client program
	 * @param args
	 */
	public static void main(String args[]){
		ClientProg client = new ClientProg();
		client.sendMessage("augment");
		System.out.println(client.recieveMessage());;
		//client.readWriteScoket();
		client.closeSocket();
		
	}

}