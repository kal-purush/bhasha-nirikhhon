package level2;

import java.util.*;

class SkynetLevel2 {

	private static String res;
	
    public static void main(String args[]) {
        Scanner in = new Scanner(System.in);
        int N = in.nextInt(); // the total number of nodes in the level, including the gateways
        int L = in.nextInt(); // the number of links
        int E = in.nextInt(); // the number of exit gateways
        
        //exits:
        int exits[] = new int[E];
        //graph:
        int graph[][] = new int[L][2];
        
        //describing links:
        for (int i = 0; i < L; i++) 
        {
            int N1 = in.nextInt(); // N1 and N2 defines a link between these nodes
            int N2 = in.nextInt();
            
            graph[i][0] = N1;
            graph[i][1] = N2;
            System.err.println(graph[i][0] + "~" + graph[i][1]);
        }
        
        System.err.println("Exits:");
        for (int i = 0; i < E; i++) {
            int EI = in.nextInt(); // the index of a gateway node
            exits[i] = EI;
        }

        // game loop
        while (true) {
            int SI = in.nextInt(); // The index of the node on which the Skynet agent is positioned this turn

            if(na(SI, exits, graph, E, L) == true){         
                System.err.println("Agent is close");
            }
                        
            else {         
                dor(SI, exits, graph, E, L);
                System.err.println("Agent is far");
            }  
        }   
    }
    
    private static boolean na(int SI, int exits[], int graph[][], int E, int L) {
    	for(int i = 0; i < E; i++)
        {     
        	for(int j = 0; j < L; j++){
        		if(SI == graph[j][0] && exits[i] == graph[j][1]){
        			res = String.valueOf(graph[j][0]) + " " + String.valueOf(graph[j][1]);  
        			System.out.println(res);
        			graph[j][0] = -1;
        			graph[j][1] = -1;
        			return true;
        			
        		} 
        		else if(SI == graph[j][1] && exits[i] == graph[j][0]){
        			res = String.valueOf(graph[j][0]) + " " + String.valueOf(graph[j][1]);    
        			System.out.println(res);
        			graph[j][0] = -1;
        			graph[j][1] = -1;
        			return true;
                }  		
        	}
        }
		return false;
	}
    
    private static void dor (int SI, int exits[], int graph[][], int E, int L){
    	
    	//severe link close to the agent
    		for(int j = 0; j < L; j++){  
    	        if(graph[j][0] == SI || graph[j][1] == SI){
		            res = String.valueOf(graph[j][0]) + " " + String.valueOf(graph[j][1]); 
		            System.out.println(res);
		            graph[j][0] = -1;
		            graph[j][1] = -1;
		            break;
	            } 
    	    }
    }
}