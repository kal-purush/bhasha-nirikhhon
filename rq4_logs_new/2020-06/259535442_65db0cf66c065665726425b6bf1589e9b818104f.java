import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class ChatServer {
	public static void main(String[] args) {
		try {
			ServerSocket server = new ServerSocket(10001);
			System.out.println("������ ��ٸ��ϴ�.");
			HashMap<String, PrintWriter> hm = 
					new HashMap<String, PrintWriter>();
			while(true) {
				Socket sock = server.accept();
				ChatThread chatthread = new ChatThread(sock, hm);
				chatthread.start();
			}
		} catch (Exception e) {
			System.out.println("server main : " + e);
		}
	}
}