
import java.net.*;

public class Main {

	public static void main(String[] args) {
        int nreq = 1;
        try
        {
        	ServerSocket sock = new ServerSocket (Integer.parseInt(args[0]));
        	System.out.println("Server started on port " + args[0]);
            for (;;)
            {
                Socket newsock = sock.accept();
                System.out.println("Creating thread ...");
                Thread t = new ThreadHandler(newsock,nreq);
                t.start();
                nreq++;
            }
        }
        catch (Exception e)
        {
            System.out.println("IO error " + e);
        }
        System.out.println("End!");
	}

}