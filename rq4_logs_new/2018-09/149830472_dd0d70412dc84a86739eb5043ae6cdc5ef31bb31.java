//package project;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

public class Client {
public static void main(String[] args) {
	try {Socket s = new Socket("35.39.165.69",6666);
	DataOutputStream dout= new DataOutputStream(s.getOutputStream());
	DataInputStream dis= new DataInputStream(s.getInputStream());
	Scanner sc = new Scanner(System.in);
	System.out.println("Connected..");
	System.out.println("your are connected to "+s.getRemoteSocketAddress().toString());
	System.out.println("press q to exit the program");
	String word =" ";
	
	while(!word.equals("q")) {
	
	dout.writeUTF(word);
	Runnable runnable = ()->{
	try {
		System.out.println(dis.readUTF());
		dout.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
		};
		runnable.run();
		word = sc.nextLine();
		
	}
	
	dout.close();
	s.close();
	}catch(Exception e) {System.out.println(e);}
}
}