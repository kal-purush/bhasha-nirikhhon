import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Date;
import java.util.Random;

public class PingServer {

	/* Variable declarations. */
	private DatagramSocket socket;
	private int PORT_NUMBER = 5533;
	private int PACKET_SIZE = 512;
    private byte[] buffer = new byte[PACKET_SIZE];
    private double LOSS_RATE = 0.3;
    private int AVERAGE_DELAY = 100;
    
    /* Methods */
    
    /* 
     * Constructor creates a connection to a DatagramSocket using PortNumber. 
     */
    public PingServer() {
    	
    	try {
			socket = new DatagramSocket(PORT_NUMBER);
		} catch (SocketException e) {
			System.out.println("Error: Server side Datagram Socket could not be opened.");
			e.printStackTrace();
		}
    }
    
    /*
     * Generates a random number and determines whether or not their should
     * be a packet loss, resulting in the return of the appropriate boolean value.
     */
    public boolean losePackets() {
    	
    	boolean packetLoss;
    	
    	// Generate a random number between 0 and 1.
        Random random = new Random(new Date().getTime());
        if (random.nextFloat() < LOSS_RATE) {
        	packetLoss = true;
        } else {
        	packetLoss = false;
        }
        return packetLoss;
    }
    
    /*
     * Runs the PingServer.
     */
    public void run() {
    	
    	while (true) {
    		
    		DatagramPacket packet = new DatagramPacket(buffer, PACKET_SIZE);
    		try {
				socket.receive(packet);
			} catch (IOException e) {
				System.out.println("Error: Server side Datagram Socket could not receive packet.");
				e.printStackTrace();
			}
    		
    		//simulate transmission delay; DOUBLE = 2
			Random random = new Random(new Date().getTime());
    		try {
				Thread.sleep((int)(random.nextDouble() * 2 * AVERAGE_DELAY));
			} catch (InterruptedException e) {
				System.out.println("Error: Failed to call Thread.sleep()");
				e.printStackTrace();
			}
    		
    		InetAddress address = packet.getAddress();
    		int port = packet.getPort();
    		packet = new DatagramPacket(buffer, buffer.length, address, port);
    		
    		try {
				socket.send(packet);
			} catch (IOException e) {
				System.out.println("Error: Failed to send packet.");
				e.printStackTrace();
			}
    		
    		socket.close();
    		

    	}
    	
    }
    
}