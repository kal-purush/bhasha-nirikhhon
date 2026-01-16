package group6;

import java.awt.BorderLayout;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.text.DefaultCaret;


public class ServerThread extends Thread {
	
	DatagramSocket socket;
	int port;
	InetAddress IPAddress;

	private byte[] receiveData;
	private int count;
	
	private JTextField field;
	private JTextArea area;
	private JFrame frame;
	private JScrollPane scrollPane;
	private JPanel textPanel, controlPanel;
	private JButton sendToApp, sendToDatabase;
	

	public ServerThread(int port, InetAddress ip) throws SocketException {
		this.IPAddress = ip;
		this.port = port;
	
		socket = new DatagramSocket();
		receiveData = new byte[512];
		initGUI();
		
	}
	
	public void sendPacket(byte[] sendData, int port, String ipString) throws IOException{
		InetAddress ip = InetAddress.getByName(ipString);
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, ip, port);	   
		socket.send(sendPacket);
	}
	
	
	public void run(){
		String  s = "ok done";
			   
		try {
			DatagramPacket sendPacket = new DatagramPacket(s.getBytes(), s.getBytes().length, IPAddress, port);
			this.socket.send(sendPacket);
			System.out.println("\n Server thread is accepting connection");
			 while(true){
	        		
	        		DatagramPacket receivePacket = new DatagramPacket(this.receiveData, this.receiveData.length);	
	    			socket.receive(receivePacket);
	    			sendPacket(receivePacket.getData(), 6799, "172.17.88.251");

	                System.out.println("Packet [" + this.count + "] arrived with length " + receivePacket.getData().length);
	                this.count++;
	                String str = new String(receivePacket.getData()).trim();
	                System.out.println(str);
	    			area.append(str + " ppm \n");   
	            if(!socket.isBound()){
	            	  break;
	            }
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	
	public void initGUI(){
		frame = new JFrame("Server");
		frame.setSize(350,500);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		//list = new ArrayList<JButton>();
		frame.getContentPane().setLayout(new BorderLayout());
		
		textPanel = new JPanel();
		field = new JTextField("Values frome the sensor ");
		//textPanel.add(field);
		frame.getContentPane().add(field, BorderLayout.NORTH) ;
		area = new JTextArea(25,15);
	
		DefaultCaret caret = (DefaultCaret) area.getCaret(); 
		caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);      

		scrollPane = new JScrollPane(area);
		scrollPane.setViewportView(area);
		
		textPanel.add(scrollPane);
		frame.getContentPane().add(textPanel, BorderLayout.CENTER) ;
		
		controlPanel = new JPanel();
		frame.getContentPane().add(controlPanel, BorderLayout.SOUTH) ;
		frame.setVisible(true);
	}

	
}