import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class ServidorMultiCliente {

	 static ServerSocket sk;
	 
	 
	 
	 
	 static class ServerThread implements Runnable {
		    Socket client = null;
		    public ServerThread(Socket c) {
		        this.client = c;
		    }
		    public void run() {
		        try {
		            System.out.println("Connected to client : "+client.getInetAddress().getHostName());
		            
		            do{
		                 BufferedReader entrada = new BufferedReader(new InputStreamReader(client.getInputStream()));
		                 PrintWriter salida = new PrintWriter(new OutputStreamWriter(client.getOutputStream()),true);
		                
		                 String datos = entrada.readLine();
		                  System.out.println("Movil: "+client.getInetAddress().getHostName()+" : "+datos);
		                  salida.print(datos);
		                  
		                 }while(true);
		        } catch (Exception e) {
		            System.err.println(e.getMessage());
		        }
		    }
		    }
	 
	 
	 
	 public static void main(String args[]) {
			 
		 
		 Boolean Serv=true;
		int Puerto=9001;
		   try {
		          sk = new ServerSocket(Puerto);
		          System.out.println();  
	        	     System.out.println("*****************************************************");
			        System.out.println("************    ServidorMulticliente      ***********");
	                  System.out.println("************    IP: "+InetAddress.getLocalHost().getHostAddress()+":"+Puerto+"     **********");
	                 System.out.println("*****************************************************");
		                 
	                 System.out.println();
		          while (Serv) { 
		        	  
		                 Socket cliente = sk.accept();
		                 new Thread(new ServerThread(cliente)).start();
		              
		          }
		   } catch (IOException e) {
		          System.out.println(e);
		   }
		  }
		

}