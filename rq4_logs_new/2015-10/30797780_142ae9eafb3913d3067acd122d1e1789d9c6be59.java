package puyo.client.human;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;

import puyo.data.Message;
import puyo.data.User;

import com.google.gson.Gson;

public enum ClientMain {
	INSTANCE;
	private String name;
	private String serverIp;
	private int port;
	private PuyoPuyoClient client;
	private DatagramSocket recSocket;

	// "127.0.0.1"
	public void sendUDP() throws IOException {
		for (Message message : client.getSendBox()) {
			DatagramSocket sendSocket = new DatagramSocket();
			InetAddress inetAddress = InetAddress.getByName(serverIp);
			Gson gson = new Gson();
			String msg = gson.toJson(message, Message.class);
			//System.out.println("sendUDP:" + msg);
			byte[] buf = msg.getBytes();
			DatagramPacket packet = new DatagramPacket(buf, buf.length, inetAddress, 5432);
			sendSocket.send(packet);
			sendSocket.close();
		}
		client.getSendBox().clear();
	}

	public void receiveUDP() throws IOException {
		if (recSocket == null) {
			recSocket = new DatagramSocket(port);
		}
		byte[] buf = new byte[1024 * 5];
		DatagramPacket packet = new DatagramPacket(buf, buf.length);
		recSocket.receive(packet);
		SocketAddress sockAddress = packet.getSocketAddress();
		int len = packet.getLength();
		String msg = new String(buf, 0, len);
		if (msg.equals("")) {
			return;
		}
		Gson gson = new Gson();
		Message message = gson.fromJson(msg, Message.class);
		client.getReceivedBox().add(message);
		//System.out.println("receiveUDP:" + msg);
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	public void setClient(PuyoPuyoClient client) {
		this.client = client;
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("args : name serverIp self-port");
			System.exit(0);
		}
		String name = args[0];
		String serverIp = args[1];
		int port = Integer.parseInt(args[2]);
		
		// キーボード入力受付用フレーム
		MainFrame frame = new MainFrame();
		frame.setTitle(name);
		frame.init();

		PuyoPuyoClient client = new PuyoPuyoClient();
		User user = new User(name, "", "", port);
		client.setUser(user);
		INSTANCE.setPort(port);
		INSTANCE.setName(name);
		INSTANCE.setServerIp(serverIp);
		INSTANCE.setClient(client);
		new Thread(() -> {
			while (true) {
				try {
					INSTANCE.receiveUDP();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		new Thread(() -> {
			while (true) {
				try {
					client.update();
					INSTANCE.sendUDP();
					Thread.sleep(1000 / 60);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

}