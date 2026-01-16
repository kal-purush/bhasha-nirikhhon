package JChatClient;

// Web Service stuff!
import javax.xml.ws.Service;
import javax.xml.namespace.QName;
import java.net.URL;
import JChatServer.ServerInterface;

// Misc!
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

public class AutoClient extends Thread {
    // Server & Service variables
    private boolean fullyInitialized = true; // True once client is up and running!
    private URL svr_url;
    private QName svr_qualName;
    private Service svr_service;
    private ServerInterface svr_endPoint;

    // Client information
    private String clientID = "";
    private String clientName = "";
    private String clientPass = "";
    private String clientAuth = "U";

    // The main method runs the actual client since we don't want it to be static!
    public static void main(String[] args) {
        // Start client
        AutoClient client = new AutoClient();
        Thread cli = new Thread(client);
        client.start();
        cli.start();
    }

    // Start the client by sorting out the server and loading the GUI.
    public void start() {
        // Debug
        if(DEBUG){System.out.println("Client started");}

        // Set the URL and qualified name
        // Debug
        if(DEBUG){System.out.print("\tSetting URL and QName...");}
        try {
            this.svr_url = new URL("http://localhost:8081/ChatterBox?wsdl");
            this.svr_qualName = new QName("http://JChatServer/", "ServerService");

            // Debug
            if(DEBUG){System.out.println("Success!");}
        } catch(Exception e){
            // Debug
            if(DEBUG){System.out.println("Error!\n\t" + e.getMessage());}
            System.exit(1);
        }

        // Get the service port/endpoint.
        this.svr_service = Service.create(this.svr_url, this.svr_qualName);
        this.svr_endPoint = this.svr_service.getPort(ServerInterface.class);

        // Generate the client passPhrase - this is for security!
        // By generating a unique ID, we can ensure that nobody can pretend to be
        // us, although with a bit of tinkering and reverse engineering,
        // it is, technically, possible.
        UUID pp = UUID.randomUUID();
        this.clientPass = pp.toString();
        if(DEBUG){System.out.println("\tClientPass: " + this.clientPass);}

        // Now we need to connect to the chat server and get our ID and name.
        if(DEBUG){System.out.print("\tAttemping to get client ID...");}
        try {
            while(this.clientID.equals("")){
                System.out.print(".");
                String[] getInfo = this.svr_endPoint.join(this.clientPass);
                this.clientID = getInfo[0];
                this.clientName = getInfo[1];
            }

            // Debug
            if(DEBUG){
                System.out.println("Success!");
                System.out.println("\tClientID: " + this.clientID);
                System.out.println("\tClientName: " + this.clientName);
            }
        } catch(Exception e){
            System.out.print("Error!\n\t" + e.getMessage());
            System.exit(1);
        }
    }

    // Quit the chat
    private void quit(){
        try {
            this.svr_endPoint.quit(this.clientID, this.clientPass);
        } catch(Exception e){
            // We don't actually care here because we're quitting anyway!
        }
        System.exit(1);
    }

    // The run method for the thread - continually sends messages and performs actions!
    public void run(){
        // If we don't have a client ID yet, it means we aren't a client! So wait
        while(this.clientID.equals("") && !this.fullyInitialized){
            // Do nothing
            if(DEBUG){System.out.println("Waiting to become client & initialize display...");}
        }

        // Set some pre-defined things!
        Random randomGenerator = new Random();
        ArrayList<String> things = new ArrayList<String>();
        things.add("/nick AutoBot_" + randomGenerator.nextInt(100)); // Name self
        things.add("/me is the best AutoBot in town!");
        things.add("/msg AutoBot_" + randomGenerator.nextInt(100) + " You aren't a good AutoBot like me!"); // Message another auto bot
        things.add("/msg Anthony Hello!");
        things.add("/auth samaria");
        things.add("/deauth");
        things.add("I'm so cool!");
        things.add("Hell yeah! We rock!!!!");
        things.add("How cool are we, eh?!");

        // Now that we're a client, let's poll the server for things.
        while(true){
            try {
                // Randomly get a new thing to do!
                int action = randomGenerator.nextInt(things.size()-1);

                // Perform the action!
                this.svr_endPoint.talk(this.clientID, this.clientPass, things.get(action));
            } catch(Exception e){
                System.out.println(e.getMessage());
            }

            // Try and sleep
            try {
                Thread.sleep(1500);
            }  catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
    }

    // All of the constants for the client.
    private static final boolean DEBUG = true;
    private static final String WINDOW_TITLE = "Chatterbox :: Currently logged in as ";
    private static final int FRAME_WIDTH = 600;
    private static final int FRAME_HEIGHT = 500;
    private static final int PADDING = 10;
    private static final int MESSAGE_WIDTH = 450;
    private static final int MESSAGE_HEIGHT = 400;
}