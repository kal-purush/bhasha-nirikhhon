import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.text.SimpleDateFormat;

public class rscmnd
{	
	public static void main(String[] args) throws Exception
	{
		//Checking for Help command
		if(args[0].equals("-h help"))
		{
			echo("Execution commands: java rscmnd port_number help");
            echo("where:" + "\n" + "a. 'java rscmnd' is for running the executable of server source code");
            echo("b. 'port-number' is the port number at which the server will listen to for connections (use port number greater than 10000 to be in safer side)");
            echo("c. 'help' will print out how to run the executable"	);
            System.exit(-1);
		}
		
		DatagramSocket socket = null;
		
		//Taking the input arguments
		int port_number = Integer.parseInt(args[1]);
			
		String s = null;
		Process p;
		
		try
		{		
			//creation of a socket with local port number
			socket = new DatagramSocket(port_number);
			
			//buffer to receive incoming data
			byte[] inputBuffer = new byte[65536];
			DatagramPacket inputstream = new DatagramPacket(inputBuffer, inputBuffer.length);
			
			echo("Server socket created. Waiting for incoming data...");
			
			StringBuilder stringS = new StringBuilder();
			
			//communication
			while(true)
			{	      
				//receive input stream from Client in socket
				socket.receive(inputstream);
				byte[] data = inputstream.getData();
				s = new String(data, 0, inputstream.getLength());
				
				InetAddress address = InetAddress.getLocalHost();
				echo("\n"+ "Accepted Client : " + address.getHostName());
				
				String[] stringArray = s.split(",");
				
				Integer executionCount = Integer.parseInt(stringArray[1]);
				Integer timeDelay = Integer.parseInt(stringArray[2]);
				
				String timeStampServer = new SimpleDateFormat("E yyyy.MM.dd 'at' HH.mm.ss").format(new java.util.Date());
				
				echo("Current Time: "+timeStampServer);
				echo("Source Client IP: " + inputstream.getAddress().getHostAddress());
				echo("Command: " + stringArray[0]);
				echo("Status: Connected" + "\n");
				echo("Command received from Client executed:" + "\n" );
				
				for(int i=1; i<=executionCount; i++)
				{
					//Execute the command
					p = Runtime.getRuntime().exec(stringArray[0]);				             
					BufferedReader buffer = new BufferedReader(new InputStreamReader(p.getInputStream()));
				    
					//Getting Timestamp at execution
					String timeStamp = new SimpleDateFormat("E yyyy.MM.dd 'at' HH.mm.ss").format(new java.util.Date());
					stringS.append("Time Stamp:" + timeStamp).append('\n');
					
					while ((s = buffer.readLine()) != null) 
					{	
						//echo the executed incoming command 
						echo(s);
						stringS.append(s).append("\n");
					}
					echo("\n");
					stringS.append("\n");
					
					//Sending data back to Client
					String toSendToClient = new String();
					toSendToClient = stringS.toString();
					
					byte[] toClient = toSendToClient.getBytes();				
					DatagramPacket dp = new DatagramPacket(toClient , toClient.length , inputstream.getAddress() , inputstream.getPort());
					socket.send(dp);
					
				    stringS = new StringBuilder();
					
					try 
					{
						Thread.sleep(timeDelay*1000);
					} 
					catch (InterruptedException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}				
            }
        }   
        catch(IOException e)
        {
            System.err.println("Exception has occurred" + e);
        }
		
		finally 
        { 
			socket.close(); 
			echo("Status: Closed");
        }
	}

	//simple function to echo data to terminal
    public static void echo(String message)
    {
        System.out.println(message);
    }
}