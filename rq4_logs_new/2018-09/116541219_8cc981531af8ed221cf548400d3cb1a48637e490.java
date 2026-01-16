package examples.chatserver;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class ChatServer extends Thread {
	private int port;
	private Set<PrintWriter> socketSet = null;
	
	public ChatServer(int port) {
		this.port = port;
		socketSet = new CopyOnWriteArraySet();
	}
	@Override
	public void run() {
		// chat server do
		
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(port);
			
			while(true) {
				Socket socket = serverSocket.accept(); // Blocking method
				SocketHandler handler = new SocketHandler(this, socket);
				handler.start();
			}
		
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			try {
				serverSocket.close();
			}catch(Exception e) {}
		}
		
	}
	

}