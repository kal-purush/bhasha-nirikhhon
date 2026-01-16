package server; /**
 * Created by Matt on 11/21/2016.
 */

import java.io.IOException;
import java.net.ServerSocket;

public class JavashipsServer {
	private static final int PORT = 9876;
	private static int numberOfConnections = 0;

	private ServerSocket incoming;

	private JavashipsServer() {
		try {
			incoming = new ServerSocket(PORT);
		} catch (IOException ioe) {
			System.err.println(ioe.toString() + '\n');
			ioe.printStackTrace();
		}
	}

	public void connectPlayers() {
		while (numberOfConnections < 2) {
			try {
				new Thread(new JavashipsConnectionHandler(incoming.accept())).run();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void tellServer(final String command) {

	}

	public static void main(String[] args) {
		JavashipsServer jss = new JavashipsServer();
	}
}

