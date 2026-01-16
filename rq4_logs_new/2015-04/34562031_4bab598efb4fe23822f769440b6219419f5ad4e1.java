import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;


public class StupidConnection {
	private PrintWriter out;
	private BufferedReader in;
	private Socket socket;
	private String hostname;
	
	public static void main(String[] args) throws UnknownHostException, IOException {
		StupidConnection conn = new StupidConnection("141.82.57.23");
		conn.start(System.out);
	}
	
	public StupidConnection(String hostname) throws UnknownHostException, IOException {
		this.hostname = hostname;
	}
	
	public void start(PrintStream output) throws UnknownHostException, IOException {
		socket = new Socket(hostname, 12810);
		
		try {
			
			out = new PrintWriter(socket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			
			out.print("This is STUPID/1.0\r\n");
			out.print("Name: Philipp Weber\r\n");
			out.print("\r\n");
			out.flush();
			
			List<String> response = receiveMesssage();
			
			int status = checkHeader(response.get(0));
			switch(status) {
				case 100:
					out.println("Connection established");
					break;
				case 300:
					throw new StupidException("Malformed packet");
				case 500:
					out.println("You already have passed, stopping");
					close();
					return;
				default: 
					throw new StupidException("Unexpected status code " + status);
			}
			
			int number = 0;
			String str;
			boolean found = false;
			
			for(int i = 1; i < response.size(); i++) {
				str = response.get(i);
				if(str.startsWith("Number: ")) {
					str = str.substring(8, str.length() - 2);
					try {
						number = Integer.parseInt(str);
					} catch(NumberFormatException e) {
						throw new StupidException("strange Number " + str);
					}
					number *= 42;
				}
			}
			
			if(!found) {
				throw new StupidException("Number Field not found");
			}
			
			out.print("This is STUPID/1.0\r\n");
			out.print("Result: " + number + "\r\n");
			out.print("\r\n");
			out.flush();
			
			response = receiveMesssage();
			
			status = checkHeader(response.get(0));
			switch(status) {
				case 200:
					out.println("Test Passed, exiting");
				case 300:
					throw new StupidException("Malformed packet");
				default: 
					throw new StupidException("Unexpected status code " + status);
			}
			

		} catch (IOException e) {
			throw e;
		} catch (StupidException e) {
			out.println(e.getMessage());
		} finally {
			close();
		}
		
		
	}
	
	private void close() throws IOException {
		in.close();
		out.close();
		socket.close();
	}
	
	private List<String> receiveMesssage() throws IOException {
		String newLine;
		List<String> message = new ArrayList<String>();
		while ((newLine = in.readLine()) != null) {
		    message.add(newLine);
		    if (newLine.equals(""))
		        break;
		}
		return message;
	}

	
	private int checkHeader(String header) {
		if(!header.startsWith("STUPID/1.0"))
			throw new StupidException("Wrong protocoll");
		try {
			return Integer.parseInt(header.split(" ")[1]);
		} catch(NumberFormatException e) {
			throw new StupidException("Faulty staus code");
		}
	}
}

@SuppressWarnings("serial")
class StupidException extends RuntimeException {
	public StupidException(String message) {
		super(message);
	}
}