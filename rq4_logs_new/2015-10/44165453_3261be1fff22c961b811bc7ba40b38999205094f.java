package Client;

import java.net.Socket;

public class ClientToClient implements Runnable{

    private ClientFrame peerChat;
    private String hostIP;
    private int hostPort;
    private Socket peer;    

    public ClientToClient(ClientFrame peerChat, String hostIP, int hostPort) {
        this.peerChat = peerChat;
        this.hostIP = hostIP;
        this.hostPort = hostPort;
    }

    public ClientToClient(ClientFrame peerChat, Socket peer) {
        this.peerChat = peerChat;
        this.hostIP = hostIP;
        this.hostPort = hostPort;
        this.peer = peer;
    }
    
    @Override
    public void run() {
        try {
            if (peer == null) peer = new Socket(hostIP, hostPort);
            //TODO: implement communicate between client and client
        }
        catch (Exception e) {
            System.out.println("Exception ClientToClient: run()");
        }        
    }
    
}