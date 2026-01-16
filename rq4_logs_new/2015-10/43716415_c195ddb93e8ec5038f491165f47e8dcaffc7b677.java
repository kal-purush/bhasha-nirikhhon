import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.*;



public class Project1{
	//field of the project: 
	public static String config_file;
	//public static final String HOST_SUFFIX = ".utdallas.edu";
	public static int number_nodes; //record the number of nodes in the system
	public static final String my_net_id = "xxw130730";
	public static String net_id_config;
	public static String nodeID; 
	//Section 2 messages:
	static ArrayList<String> nodeNames = new ArrayList<String>();
	static ArrayList<String> hostNames = new ArrayList<String>();
	static ArrayList<String> portNums = new ArrayList<String>();
	//Section 3 messages:
	static ArrayList<String> tokenPaths = new ArrayList<String>();
	
	static int random_value;
	static ServerSocket serverSock;
	static PrintWriter writer_file;
	static volatile int token_received;
	static int token_emitted;
	static boolean myTokensRunning = true;
	static boolean otherTokensRunning = true; 
	static int countReceive = 0;

	
	public static void main(String[] args){
		System.out.println();
		if(args.length == 0)
		{
			System.out.println("Please input the node ID");
			return;
		}
		token_received = 0;
		token_emitted = 0;
		nodeID = args[0];
		config_file = args[1];
		//System.out.println("Node ID: " + nodeID);
		
		Random randomGenerator = new Random();
		random_value = randomGenerator.nextInt(10000);
		//System.out.println("Random value of this node: " + random_value);
		
		Project1 project1 = new Project1();
		
		readConfig(Integer.parseInt(nodeID));
		
		System.out.println("Started, this program takes about 6 seconds to finish, please remain patient...");
		System.out.println("Constructing output file...");
		try{
					
			writer_file = new PrintWriter(config_file.replace(".txt", "")+"-xxw130730-"+ nodeID +".out");
			writer_file.println("Net ID: xxw130730");
			writer_file.println("Node ID: " + nodeID);
			writer_file.println("Listening on " + hostNames.get(Integer.parseInt(nodeID))+": "+portNums.get(Integer.parseInt(nodeID)));
			writer_file.println("Random Number: " + random_value);
			if(tokenPaths.size() == 0) {
				writer_file.println("All tokens received");
				writer_file.close();
			}
			
		}catch(IOException e){
			e.printStackTrace();
		}
		
		enableServer();
		System.out.println();
		sleep(5);
		emitTokens();
		project1.listenSocket();
		System.out.println("out of listenSocket()");
		
		return;
	}
	
	static void readConfig(int node_ID){
		System.out.println("Node "+ node_ID + ": Starting to read config file!");
		System.out.println();
		
		try(BufferedReader br = new BufferedReader(new FileReader(config_file)))
		{
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				//empty line case:
				if(currentLine.trim().length() == 0) {continue;}
				if(currentLine.trim().charAt(0) == '#'){continue;}
				if(currentLine.trim().charAt(0) != '#' && currentLine.trim().contains("#")){
					currentLine = currentLine.substring(0, currentLine.indexOf('#'));
				}
				//section 1 : xx xxw130730:
				if(currentLine.contains(" xxw130730")){
					net_id_config = "xxw130730";
					//System.out.println("Section 1: number of nodes and netid ");
					number_nodes = Integer.parseInt(currentLine.trim().replaceAll("\\s+", " ").replace(" xxw130730",""));
					//System.out.println("number of nodes: " + number_nodes);	
					//System.out.println("net_id from config file: " + net_id_config);
					//System.out.println();
				}else if(currentLine.contains(" kam093020")){
					net_id_config = "kam093020";
					//System.out.println("Section 1: number of nodes and netid ");
					number_nodes = Integer.parseInt(currentLine.trim().replaceAll("\\s+", " ").replace(" kam093020",""));
					//System.out.println("number of nodes: " + number_nodes);	
					//System.out.println("net_id from config file: " + net_id_config);
				}
				//section 2 : node_id host_id port:
				else if(currentLine.contains("dc")){
					String after = currentLine.trim().replaceAll("\\s+", " ");
					String[] parts = after.split("\\s+");
					nodeNames.add(parts[0]);
					hostNames.add(parts[1]);
					portNums.add(parts[2]);
					//System.out.println("Section 2: ");
					//System.out.println("node, host, port: " + parts[0]+ ", " + parts[1] + ", " + parts[2]);
				}else{//assume clean config file: 
					//System.out.println("Section 3: token paths: ");
					if(!currentLine.trim().contains(" ")){
						if(Integer.valueOf(currentLine.trim()) == Integer.valueOf(nodeID)){
							tokenPaths.add(currentLine.trim());

						}
					}else{
						String[] paths = currentLine.trim().replaceAll("\\s+", " ").split("\\s+");
						
						if(Integer.valueOf(paths[0]) == Integer.valueOf(nodeID)) tokenPaths.add(currentLine.trim().replaceAll("\\s+", " "));
					}
				}
			}
			//System.out.println();
			//System.out.println("Section 3: token paths: ");
			//for(int j=0; j<tokenPaths.size();j++){
				//	System.out.println("token paths taken by node " + nodeID + ": " + tokenPaths.get(j));
				//}
			//System.out.println("Number of tokens: " + tokenPaths.size());
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	static void enableServer(){
		int port = 0;
		try{
			for(int i=0; i<nodeNames.size(); i++){
				if(Integer.valueOf(nodeID) == Integer.valueOf(nodeNames.get(i))){
					port = Integer.parseInt(portNums.get(i));
				}
			}
			serverSock = new ServerSocket(port);
			//System.out.println("Node " + nodeID + " listening on port " + port);
		}catch (IOException e){
			System.out.println("Could not listen on port " + port);
			System.exit(-1);
		}
	}
	
	static void sleep(int seconds){
		try {
			Thread.sleep(seconds * 1000);
		}catch(InterruptedException ex){
			Thread.currentThread().interrupt();
		}
	}
	
	static void emitTokens(){
		
		
		if(tokenPaths.size() == 0){
			System.out.println("No tokens to be sent for this node ");
			myTokensRunning = false;
			broadcastReceived();
			
			return;
		}else{
			
			for(int i = 0; i<tokenPaths.size(); i++){
				token_emitted++;
				//handling first batch of tokens:
				//erase itself from path, add itself to the end of the token, add value and paths
				String oldToken = new String(tokenPaths.get(i) + " " + random_value + " " + nodeID + " " + token_emitted);
				//delete myslef on the path and send
				String newToken = oldToken.substring(oldToken.indexOf(' ')+1);
				
				String[] parts = newToken.split("\\s+");
				
				//need to write to output file now:
				StringBuilder writeString = new StringBuilder();
				writeString.append(tokenPaths.get(i) + " " + nodeID);
				String writeFile = writeString.toString();
				writeFile = writeFile.replace(" "," -> ");
				writer_file.println("Emitting token " + token_emitted + "   with path " + writeFile);
				
				
				if(parts.length == 3){
					System.out.println("This node is the only node on the path: " + newToken);
					//loopback to itself:
					int self_node = Integer.parseInt(parts[1]); 
					//parts[1] is node itself,same as below //int self_node = Integer.parseInt(nodeID);
					int self_port = Integer.parseInt(portNums.get(self_node));
					String self_host = new String(hostNames.get(self_node) + ".utdallas.edu");
					
					//System.out.println("Token info for this node:  host , token");
					//System.out.println( self_host + ", " + newToken );
					//System.out.println("Emitting token to itself... ");
						
					connectAndSend(self_host, self_port, newToken);						
					
				}else if(parts.length > 3){
					//need to send the newToken to next node:
					int next_node = Integer.parseInt(parts[0]);
					int next_port = Integer.parseInt(portNums.get(next_node));
					String next_host = new String(hostNames.get(next_node) + ".utdallas.edu");
					//System.out.println("Token info for this node:  host , token");
					//System.out.println(next_host + ", " + newToken);
					//System.out.println("Emitting token... ");
					//emit token:
					
					connectAndSend(next_host, next_port, newToken);
					
				}
				
			}
			
		}
		
		return;
	}
	
	void listenSocket(){
		while( myTokensRunning || otherTokensRunning ){//change this to all other nodes tokens received later
			ClientWorker w;
			try{
				w = new ClientWorker(serverSock.accept());
				Thread t = new Thread(w);
				t.start();
				
			}catch(IOException e){
				System.out.println("Accept failed in listenSocket(), node "+nodeID+" terminated");
				try {
						serverSock.close();
						Thread.currentThread().interrupt();
					}catch(IOException ex){
						System.out.println("this node has already terminated, node : " + nodeID);
					}
				System.exit(-1);
			}
		}	
	}
	
	class ClientWorker implements Runnable {
		private Socket client;

		//Constructor
		ClientWorker(Socket client) {
			this.client = client;
		}
		
		public void run(){
			String line;
			BufferedReader in = null;
			PrintWriter out = null;
			try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(), true);
			}catch (IOException e) {
				System.out.println("in or out failed");
				System.exit(-1);
			}
			
			//while(true){
				try{
					line = in.readLine();
					if(line == null) {
						client.close();
						return;
					}
					
					if(line.contains("AllReceived")){
						countReceive++;
						if( countReceive == nodeNames.size()-1 ){
							System.out.println("Done, please press enter and run cleanup.sh");
							otherTokensRunning = false;
							//System.out.println("myTokensRunning and otherTokensRunning: " + myTokensRunning + " " + otherTokensRunning);
							/*if((!myTokensRunning) && (!otherTokensRunning)){
								try {
										serverSock.close();
										Thread.currentThread().interrupt();
										//System.exit(-1);
									}catch(IOException e){
										//log error just in case
									}
							}*/
							//return;
						}
						client.close();
						Thread.currentThread().interrupt();
						
						return;
					}
					
					System.out.println("Token received: " + line);
					String[] parts = line.split("\\s+");
					//myself is the last node on the token path: length == 4
					//need to send it to the origin node and plus my random value
					if(parts.length == 4){
						
						if(Integer.parseInt(parts[0])!= Integer.parseInt(nodeID)){
							int final_correct_node = Integer.parseInt(parts[0]);
							int final_correct_port = Integer.parseInt(portNums.get(final_correct_node));
							String final_correct_host = hostNames.get(final_correct_node) + ".utdallas.edu";
							connectAndSend(final_correct_host, final_correct_port, line);
							client.close();
							Thread.currentThread().interrupt();
							return;
						}
						
						int old_value = Integer.parseInt(parts[parts.length -3].trim());
						int last_value = old_value + random_value;
						int origin_node = Integer.parseInt(parts[2]);
						int origin_port = Integer.parseInt(portNums.get(origin_node));
						String origin_host = new String(hostNames.get(origin_node) + ".utdallas.edu");
						//modify token
						String last_token = line.substring(line.indexOf(' ')+1);
						last_token = last_token.replace(Integer.toString(old_value), Integer.toString(last_value));
						//forward the token to its origin point:
						connectAndSend(origin_host, origin_port, last_token);
						//System.out.println("Random value of this node: " + random_value);
						//System.out.println("Old token value: " + old_value);
						//System.out.println("After sum: " + last_value);
						
					}else if(parts.length > 4){
						
						if(Integer.parseInt(parts[0])!= Integer.parseInt(nodeID)){
							int correct_node = Integer.parseInt(parts[0]);
							int correct_port = Integer.parseInt(portNums.get(correct_node));
							String correct_host = hostNames.get(correct_node) + ".utdallas.edu";
							connectAndSend(correct_host, correct_port, line);
							client.close();
							Thread.currentThread().interrupt();
							return;
						}
						//normal condition when the token paths are long enough
						int pre_value = Integer.parseInt(parts[parts.length - 3].trim());
						int current_value = pre_value + random_value;
						int next_node = Integer.parseInt(parts[0]);
						int next_port = Integer.parseInt(portNums.get(next_node));
						String next_host = new String(hostNames.get(next_node) + ".utdallas.edu");
						String next_token = line.substring(line.indexOf(' ')+1);
						next_token = next_token.replace(Integer.toString(pre_value), Integer.toString(current_value));
						
						connectAndSend(next_host, next_port, next_token);
						//System.out.println("Random value of this node: " + random_value);
						//System.out.println("Old token value: " + pre_value);
						//System.out.println("After sum: " + current_value);
						
					}else if(parts.length == 3){
						//the token return to its origin node. add nothing, only take the value out
						token_received++;
						int ultimate_value = Integer.parseInt(parts[0].trim());
						//System.out.println("A token received by node " + nodeID + ", value: " + ultimate_value);
						writer_file.println("Received token " + parts[parts.length-1] + "   Token sum: " + parts[0]);
						if(token_received == tokenPaths.size()){
							//now it needs to tell other nodes that it received all tokens!
							myTokensRunning = false;
							broadcastReceived();
							writer_file.println("All tokens received");
							writer_file.close();
						}
						
					}else{
						System.out.println("Error: parts.length < 2 , impossible condition");
					}
					//boooooooo:
					client.close();
					Thread.currentThread().interrupt();
					return;
					//break;
				}catch(IOException e){
					System.out.println("Read failed");
					System.exit(-1);
				}
			//}
		}
	}
	
	static void connectAndSend(String host, int port, String token){
		try{		
			Socket ClientSocket = new Socket(host, port);
			PrintWriter writer = new PrintWriter(ClientSocket.getOutputStream(), true);//boolean autoflush or not?
			writer.println(token);
			writer.close();	
			ClientSocket.close();
		}catch(IOException ex){
			System.out.println("Exception errors from ClientWorker class run() or emitTokens()");
			System.out.println("Failed to connect to server or write to server ");
			ex.printStackTrace();
		}
	}
	
	static void broadcastReceived(){
		String message = "AllReceived";
		for(int i=0; i < nodeNames.size();i++){
			int intNode = Integer.parseInt(nodeID);
			if( i != intNode){
				int port = Integer.parseInt(portNums.get(i));
				String host = hostNames.get(i)+".utdallas.edu";
				connectAndSend(host, port, message);
			}
		}
		
	}
	
	
}