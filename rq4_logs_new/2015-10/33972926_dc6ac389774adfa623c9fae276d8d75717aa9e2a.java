/*
 * This class provides the actual core of the ESS.
 * It accepts and completes connections for data and changes in datasets.
 */

package experimentalserverservice2;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 *
 * @author mgohde
 */
public class DataServer
{
    private Thread listenerThread;
    private ArrayList<Experiment> experimentList;
    ServerSocket srvrSock;
    private Logger l;
    private int port;
    
    /**
     * Default constructor for the DataServer. 
     * @param l
     * @param expList
     * @param port 
     */
    public DataServer(Logger l, ArrayList<Experiment> expList, int port)
    {
        listenerThread=null;
        experimentList=expList;
        this.port=port;
    }
    
    /**
     * 
     * @throws IOException 
     */
    public void start() throws IOException
    {
        if(listenerThread==null)
        {
            srvrSock=new ServerSocket(port);
            listenerThread=new Thread(new ListenerRunnable(srvrSock));
        }
    }
    
    public void stop()
    {
        if(listenerThread!=null)
        {
            try
            {
                srvrSock.close();
            } catch(IOException e)
            {
                //Do nothing. The socket should still be closed.
            }
            
            listenerThread=null;
        }
    }
    
    private class ListenerRunnable implements Runnable
    {
        ServerSocket sock;
        
        @Override
        public void run()
        {
            while(true)
            {
                try
                {
                    Socket s=sock.accept();
                    l.logMsg("ConnectionListener", "Accepting connection...");
                    new Thread(new ConnectionRunnable(new Connection(s))).start();
                } catch(IOException e)
                {
                    break;
                }
            }
        }
        
        public ListenerRunnable(ServerSocket socket) 
        {
            sock=socket;
        }
    }
    
    private class ConnectionRunnable implements Runnable
    {
        private Connection internalConnection;
        
        @Override
        public void run()
        {
            ESSProtocolServer s=new ESSProtocolServer(internalConnection, experimentList, l);
            s.serve();
            internalConnection.close();
        }
        
        public ConnectionRunnable(Connection c)
        {
            internalConnection=c;
        }
    }
}