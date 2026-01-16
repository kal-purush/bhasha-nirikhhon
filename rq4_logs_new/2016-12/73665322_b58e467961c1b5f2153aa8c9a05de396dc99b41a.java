package iot;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import javax.xml.soap.Node;

//import iot.ManageDB;

public class ListenThread implements Runnable{
	public 	static HashMap<String, String>  map= new HashMap<>();

	public static HashMap<Integer, Nodes> receivedNodesMap = new HashMap<>();
static	Boolean status =false;
public static String [] numbersArray;
	Thread thread = new Thread(this);
	static	String str = null;
	public ListenThread(){
		thread.start();
	}
	public void run(){
	
			
		try{
			BufferedReader in = null;
			String ack = "received";
			
			ServerSocket listener = new ServerSocket(6111);
			listener.setReuseAddress(true);
			while (true) {
		System.out.println("waiting for tx");
				Socket socket = listener.accept();
System.out.println("received tx");


in = new BufferedReader(
		new InputStreamReader(socket.getInputStream()));
str = in.readLine();


//ListenThread.map.put(numbersArray[0], numbersArray[1]);
if(str.equalsIgnoreCase("null")){
	System.out.println("inside null");
	}
	else{
System.out.println("Inside not null");
		numbersArray = str.split(" ");

		int length = numbersArray.length;
		//int size = length/4;
int a =0;

		for(int i = 0; i < length; i=i+4){
			
			String ipAdd= numbersArray[i];
			String sysName = numbersArray[i+1];
			String macAdd = numbersArray[i+2];
			String role = numbersArray[i+3];
			Nodes nodes = new Nodes(ipAdd, sysName, macAdd, role);

			receivedNodesMap.put(a, nodes);
a++;
			
		}

	//String ipaddress = str;
	//System.out.println("received ip is:-"+ipaddress);
	//status = true;
	socket.close();
	listener.close();
System.out.println("before break while");
	break;
	
	}



				}
for(int i = 0; i< receivedNodesMap.size(); i++){
	System.out.println(receivedNodesMap.get(i).ip);
	System.out.println(receivedNodesMap.get(i).sysName);
	System.out.println(receivedNodesMap.get(i).macAddress);
	System.out.println(receivedNodesMap.get(i).role);
}
//			System.out.println("role is"+nmapClass.map.get(numbersArray[0]));		
			}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	//	}
	}
}