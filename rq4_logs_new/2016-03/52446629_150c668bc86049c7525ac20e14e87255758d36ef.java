package com.mou.opencvstuff;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class Udp {
	private InetAddress addr;
	private DatagramSocket sock;
	protected int port;
	
	public Udp(String addr_, int port_) throws SocketException, UnknownHostException
	{
		port = port_;
		sock = new DatagramSocket(port);
		addr = InetAddress.getByName(addr_);
	}
	public void send(String message) throws IOException
	{
		DatagramPacket packet;
		
		packet = new DatagramPacket(
				message.getBytes(),
				message.length(),
				addr,
				port);
		sock.send(packet);
	}
}