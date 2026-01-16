package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class ClientDriver {
	
	public static void main(String[] args) {
		

		try {
			// connection
			Socket socket = new Socket("127.0.0.1", 8000);
			Runnable r = new Runnable() {
				@Override
				public void run() {

					try {
						OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream());
						out.write("Hallo");
						out.flush();
						out.close();
						
						
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					/**
					// read and save the message
					while (true) {
						//sspMsg.set(read().toJSONString());
						System.out.println(readString());
						
					}
					*/
				}
			};
			Thread t = new Thread(r);
			t.start();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

}