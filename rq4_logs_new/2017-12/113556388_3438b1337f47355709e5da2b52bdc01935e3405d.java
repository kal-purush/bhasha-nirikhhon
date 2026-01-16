package prototype;

import java.util.ArrayList;

public class Main {
	
	static int rowSize;
	static int colSize;
	
	public static void main(String[] args) {
		int[][] pixelList = new int[10][10];
		rowSize = 10;
		colSize = 10;
		
		pixelList[0] = new int[] {0,0,0,0,0,1,0,0,0,0};
		pixelList[1] = new int[] {0,0,0,0,0,1,0,0,0,0};
		pixelList[2] = new int[] {0,0,1,1,1,1,1,1,0,0};
		pixelList[3] = new int[] {0,0,1,0,0,0,0,1,0,0};
		pixelList[4] = new int[] {0,0,1,1,1,1,1,1,0,0};
		pixelList[5] = new int[] {0,0,0,0,0,0,0,1,0,0};
		pixelList[6] = new int[] {0,0,0,0,0,0,0,1,0,0};
		pixelList[7] = new int[] {0,0,0,0,0,1,1,1,0,0};
		pixelList[8] = new int[] {0,0,0,0,0,1,0,0,0,0};
		pixelList[9] = new int[] {0,0,0,0,0,1,0,0,0,0};
		
		ArrayList<Node> nodes = new ArrayList<>();
		nodes = createNodes(pixelList);
		
		for(Node node : nodes){
			String neighbours = "";
			for(Node neighbour : node.getNeighbours()){
				neighbours += "(" + neighbour.getX() + ", " + neighbour.getY() + ") ";
			}
			System.out.println("(" + node.getX() + ", " + node.getY() + ") Neighbours: " + neighbours);
		}
		
	}
	
	private static ArrayList<Node> createNodes(int[][] pixelList){
		
		ArrayList<Node> nodes = new ArrayList<>();
		
		for (int i = 0; i < pixelList.length; i++){
			for(int j = 0; j < pixelList[i].length; j++){
				int pixel = pixelList[i][j];
				
				//Skipping black pixels
				if(pixel == 0){
					continue;
				}
				
				//Creating start and finish node
				if((i == 0 || i == pixelList.length -1)){
					if(i == 0)nodes.add(new Node(i, j, true, false));
					else nodes.add(new Node(i, j, false, true));					
					continue;
				}
				
				//Creating node
				if(!((pixelList[i-1][j] == 0 && pixelList[i+1][j] == 0) || (pixelList[i][j-1] == 0 && pixelList[i][j+1] == 0))){
					nodes.add(new Node(i, j));
					continue;
				}
				
			}
		}
		
		//Create edges between nodes
		createEdges(nodes, pixelList);
		
		return nodes;
	}
	
	private static ArrayList<Node> createEdges(ArrayList<Node> nodes, int[][] pixelList){
		
		for(Node node : nodes){
			
			int x = node.getX();
			int y = node.getY();
			
			
			//Check if there is a node up
			for(int i = x; i >= 0; i--){
				boolean skip = false;
				if(pixelList[i][y] == 0) break;
				for(Node checkNode : nodes){
					
					if(node.equals(checkNode)) continue;
					if(checkNode.getX() == i && checkNode.getY() == y){
						node.addNeighbour(checkNode);
						skip = true;
						break;
					}
				}
				if(skip)break;
			}
			
			
			//Check if there is a node down
			for(int i = x; i < rowSize; i++){
				boolean skip = false;
				if(pixelList[i][y] == 0) break;
				for(Node checkNode : nodes){					
					if(node.equals(checkNode)) continue;
					if(checkNode.getX() == i && checkNode.getY() == y){
						node.addNeighbour(checkNode);
						skip = true;
						break;
					}
					
				}
				if(skip)break;
			}
			
			
			//Check if there is a node to the left
			for(int i = y; i > 0; i--){
				boolean skip = false;
				if(pixelList[x][i] == 0) break;
				for(Node checkNode : nodes){					
					if(node.equals(checkNode)) continue;
					if(checkNode.getX() == x && checkNode.getY() == i){
						node.addNeighbour(checkNode);
						skip = true;
						break;
					}
					
				}
				if(skip)break;
			}
			
			
			//Check if there is a node to the right
			for(int i = y; i < colSize; i++){
				boolean skip = false;
				if(pixelList[x][i] == 0) break;
				for(Node checkNode : nodes){					
					if(node.equals(checkNode)) continue;
					if(checkNode.getX() == x && checkNode.getY() == i){
						node.addNeighbour(checkNode);
						skip = true;
						break;
					}
					
				}
				if(skip)break;
			}			
		}
		
		return nodes;
	}

}


