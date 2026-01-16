import java.awt.Color;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Client implements Runnable {
	public static final String GROUP_ADDR = "228.5.6.7";
	public static final int REMOTE_UDP_PORT = 2456;
	public static final int REMOTE_TCP_PORT = Server.LOCAL_TCP_PORT;
	private DatagramSocket socket;
	private Socket tcpSocket;
	public DataOutputStream out;
	public BufferedReader in;
	private Messages m;
<<<<<<< ours
<<<<<<< ours
	private UsersUI u;
	
	public Client(Messages m, UsersUI u) throws SocketException {
		this.m = m;
		this.u = u;
=======
=======
>>>>>>> theirs
	private Settings set;
	
	public Client(Messages m, Settings set) throws SocketException {
		this.m = m;
		this.set = set;
<<<<<<< ours
>>>>>>> theirs
=======
>>>>>>> theirs
		socket = new DatagramSocket();
		new Thread(this).start();
	}

	@Override
	public void run() {
		try {
			System.out.println("Client run method invoked");
			InetAddress group = InetAddress.getByName(GROUP_ADDR);
			
			//Add this machine's IP to packet data
			System.out.println("Client creating UDP packet...");
			String local = new String(InetAddress.getLocalHost().getHostName());
			byte[] ip = local.getBytes();
			byte[] dataToSend = new byte[ip.length+1];//NetConstants is one byte
			
			//Add data to array list
			List<Byte> dataList = new ArrayList<Byte>();
			dataList.add(NetConstants.CLIENT_AWAITING_REQUEST[0]);
			
			for (byte i : ip) dataList.add(i);	
			for (int i = 0; i < dataList.size(); i++) {
				dataToSend[i] = dataList.get(i);
			}
			System.out.println("Packet to send: "+new String(dataToSend));
			//Create and send packet
			DatagramPacket packet = new DatagramPacket(dataToSend, dataToSend.length, group, REMOTE_UDP_PORT);
			System.out.println("Client sending initial UDP packet");		
			System.out.println("Client waiting for response from server...");
			
			//Prepare for incoming data 
			byte data[];
			byte[] ipAddr;
			
			//Send packet, and if no response is given, re-send
			socket.setSoTimeout(5000);
			
			while (true) {
				System.out.println("No response from remote servers. Rebroadcasting...");
				socket.send(packet);		
				try {
					socket.receive(packet);
					data = packet.getData();
					ipAddr = Arrays.copyOfRange(data, 1, data.length);
					
					if (new String(ipAddr).equals(InetAddress.getLocalHost().getHostName()) && !packet.getAddress().getHostAddress().equals(InetAddress.getLocalHost().getHostAddress())) {
						//If the IP in the packet equals this machines', and the packet isn't from localhost
						break;
					} else { 	
						Thread.sleep(3000);
					}
				} catch (SocketTimeoutException e) {
					//Used so if there's a timeout it won't continue
					Thread.sleep(3000);
				}
			}
			
			System.out.println("Remote server found");
			data = packet.getData();
			if (data[0] == NetConstants.SERVER_REQUEST_GRANTED[0]) {
				//Packet from server grants connection and is from the right server
				System.out.println("Received appropriate response from server. Closing UDP socket");
				//UDP not needed
				socket.close();
				
				//Start TCP connection
				System.out.println("Client starting TCP...");
				tcpSocket = new Socket(packet.getAddress(), REMOTE_TCP_PORT);
				out = new DataOutputStream(tcpSocket.getOutputStream());
				in = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
				System.out.println("Waiting for a valid username to send");
				
				//Send username
<<<<<<< ours
<<<<<<< ours
				boolean isValid = false;
				while (!isValid) {
					//Wait for entry
					if (u.usrNameValid) {
						u.username.setEditable(false);
						isValid = true;
					}
					Thread.sleep(100);
				}
				System.out.println("Sending username...");
				out.writeBytes(u.username.getText()+"\n");
=======
				
				System.out.println("Sending username '"+set.getUname()+"'");
				out.writeBytes(set.getUname()+"\n");
>>>>>>> theirs
=======
				
				System.out.println("Sending username '"+set.getUname()+"'");
				out.writeBytes(set.getUname()+"\n");
>>>>>>> theirs
				
				//Make sure server allows connection (in case of identical usernames)
				System.out.println("Waiting for approval from server...");
				String confirm = in.readLine();
				if (confirm.equals(NetConstants.SERVER_REQUEST_DENIED.toString())) {
					System.out.println("Server denying further communications");
					m.setColor(Color.RED);
					m.appendString("Error: Username already taken!", true);
					
					System.out.println("Client creating new child");
<<<<<<< ours
<<<<<<< ours
					new Client(m, u);
=======
					new Client(m, set);
>>>>>>> theirs
					return;
				} else {
					System.out.println("Username accepted");
				}
				//Start new client
				System.out.println("Client creating new child");
<<<<<<< ours
				new Client(m, u);
=======
				new Client(m, set);
>>>>>>> theirs
				
				//Listen for then send messages
				while (true) {
<<<<<<< ours
					Thread.sleep(50);
=======
					new Client(m, set);
					return;
				} else {
					System.out.println("Username accepted");
				}
				//Start new client
				System.out.println("Client creating new child");
				new Client(m, set);
				
				//Listen for then send messages
				while (true) {
					Thread.sleep(300);
>>>>>>> theirs
=======
					Thread.sleep(300);
>>>>>>> theirs
					if (m.toBeSent) {
						System.out.println("Sending message '"+m.message.getText()+"' via TCP...");
						out.writeBytes(m.message.getText()+'\n');
						m.toBeSent = false;
						m.message.setText("");
					}
				}
				
			} else if (data[0] == NetConstants.SERVER_REQUEST_DENIED[0]) {
				System.out.println("Remote server denied access");
<<<<<<< ours
<<<<<<< ours
				new Client(m, u);
=======
				new Client(m, set);
>>>>>>> theirs
=======
				new Client(m, set);
>>>>>>> theirs
				return;
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			m.error(e);
		} 
	}
}