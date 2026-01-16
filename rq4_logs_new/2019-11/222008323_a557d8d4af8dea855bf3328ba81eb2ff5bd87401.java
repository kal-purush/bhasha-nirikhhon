import java.awt.EventQueue;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;

public class Pinger {

	private int MAX_PACKET_SIZE = 512;
    private DatagramSocket ds;
    private InetAddress ip;
    int timeout = 1000;
	
    public Pinger() {
    	try {
			this.ds = new DatagramSocket();
			this.ip = InetAddress.getLocalHost();
			ds.setSoTimeout(timeout);
		} catch (SocketException e) {
			System.out.println("Error building Datagram Socket.");
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("Error finding IP address of local machine.");
			e.printStackTrace();
		}
    }
    
    // Default behavior is to send 10 PING messages using UDP
    public void run() {
    	
    	for (int i=0; i < 10; i++) {
    		
    		String payload = createPayload(i);
    		
    		//Ask for Destination IP and Port
    		String host_dest = "constance.cs.rutgers.edu";
    		int port_dest = 5530;

    		// Create and send Ping message
    		InetAddress IP_dest;
			try {
				IP_dest = InetAddress.getByName(host_dest);
				PingMessage message = new PingMessage(IP_dest, port_dest, payload);
				
				// send Packet through UDP
				byte[] payloadBytes = message.getPayload().getBytes();
				DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, IP_dest, port_dest);
				ds.send(packet);
				ds.receive(packet);
				
				String socketAddress = packet.getSocketAddress().toString();
				Date date = new Date();
				long timestamp = date.getTime();
				System.out.printf("Received packet from %s %s\n", socketAddress, timestamp);
				if (i == 9) {
					timeout = 5000;
				}
				
			} catch (UnknownHostException e) {
				System.out.println("Error finding IP address from given host name.");
				System.out.println("Please try again, and enter an appropriate host name.");
				return;
			} catch (IOException e) {
				System.out.println(e.toString());
			}

    	}
    }
    
    public String createPayload(int sequenceNumber) {
    	
    	Date date = new Date();
		long timestamp = date.getTime();
		String payload = "PING " + sequenceNumber + " " + timestamp;
		
		return payload;
    }
	
	
	
}