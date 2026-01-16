package oculusbot.network.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import static oculusbot.network.NetworkConstants.*;

public class Communications {
	private DatagramSocket socket;
	private int port;
	private byte[] buf;
	private DatagramPacket in;
	private String server;

	public Communications(int port) {
		this.port = port;
		try {
			socket = new DatagramSocket();
			socket.setSoTimeout(DEFAULT_TIMEOUT);
		} catch (SocketException e) {
			e.printStackTrace();
		}
	}

	public Communications(int port, String server) {
		this(port);
		this.server = server;
	}

	public InetAddress getServerIP() {
		contactServer(OB_REQUEST_SERVER_IP, BROADCAST_IP, true);
		server = in.getAddress().getHostAddress();
		System.out.println("SERVER:"+server);
		return in.getAddress();
	}

	public void registerClient() {
		checkServerIp();
		contactServer(OB_REGISTER_CLIENT, server, false);
	}

	public void deregisterClient() {
		checkServerIp();
		contactServer(OB_DEREGISTER_CLIENT, server, false);
	}
	
	public void sendKey(int key){
		checkServerIp();
		contactServer(OB_SEND_KEY+" "+key, server, false);
	}

	private void checkServerIp() {
		if (server == null) {
			getServerIP();
		}
	}
	

	private void contactServer(String data, String ip, boolean broadcast) {
		try {
			socket.setBroadcast(broadcast);

			while (true) {
				send(data, ip);

				if (waitForAck()) {
					return;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void send(String data, String ip) throws IOException {
		System.out.print("Sending " + data + "...");
		buf = data.getBytes();
		DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(ip), port);
		socket.send(packet);
		System.out.println("DONE");
	}

	private boolean waitForAck() throws IOException {
		System.out.print("Waiting for answer...");
		buf = new byte[DEFAULT_PACKAGE_SIZE];
		in = new DatagramPacket(buf, buf.length);
		try {
			socket.receive(in);
			String msg = new String(in.getData()).trim();
			if (msg.equals(OB_ACK)) {
				System.out.println("DONE");
				return true;
			} else {
				System.err.println("ERROR: Unknown answer.");
			}
		} catch (SocketTimeoutException e) {
			System.err.println("ERROR: No response.");
		}
		return false;
	}

}