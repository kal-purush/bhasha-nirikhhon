import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;


// ClientHandler class
class ClientHandler extends Thread 
{

    final BufferedReader clientReader;
    final PrintWriter clientWriter;
    final Socket s;

    
 
    // Constructor
    public ClientHandler(Socket s, BufferedReader clientReader, PrintWriter clientWriter) 
    {
        this.s = s;
        this.clientReader = clientReader;
        this.clientWriter = clientWriter;
    }
 
    @Override
    public void run() 
    {
        String equation;
        String solution;
        while (true) 
        {
            try {
 

                //Logs the new thread in the console
                System.out.println("Client " + this.s + " established");
                 
                // receive the equation from the client
                equation = clientReader.readLine();
                 
                //if statement determines if the user entered exit, count, or an equation
                if(equation.equals("exit"))
                { 
                	//says goodbye
                	clientWriter.println("Goodybye");
                	
                	//makes log of connection lost
                    System.out.println("Client " + this.s + " sent exit signal.");
                    this.s.close();
                    System.out.println("Connection closed");
                    
                    break;
                }else{
                	
                	//echoes back the sent information
                	clientWriter.println(equation + "Received");
                }
                 

                
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
         
        try
        {
            // closing resources
            this.clientReader.close();
            this.clientWriter.close();
             
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}