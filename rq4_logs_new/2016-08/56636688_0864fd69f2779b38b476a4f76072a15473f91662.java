package level2;

import java.util.*;
import java.io.*;
import java.math.*;

class APU_Init_Phase {

    public static void main(String args[]) {
        Scanner in = new Scanner(System.in);
        int width = in.nextInt(); // the number of cells on the X axis
        int height = in.nextInt(); // the number of cells on the Y axis
        
        String nodes[][] = new String[height][width];
        
        in.nextLine();
        for (int i = 0; i < height; i++) {
            String line = in.nextLine(); // width characters, each either 0 or .
            
            for (int j = 0; j < width; j++) {
            	nodes[i][j] = line.substring(j, j+1);
            }
        } 
        
        //printing array:
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                System.err.print(nodes[i][j] + " ");
            }
            System.err.print("\n");
        }
        
        //main loop:
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {

            	if(nodes[i][j].equals("0"))
            	{
            		//finding right:
            		String rx = "-1"; 
            		String ry = "-1";
            		for (int k = j+1; k < width; k++) {
            			if(nodes[i][k].equals("0")){
            				rx = String.valueOf(k);
            				ry = String.valueOf(i);
            			}
            			break;
            		}
            		
            		//finding below:
            		String bx = "-1"; 
            		String by = "-1";
            		for (int m = i+1; m < height; m++) {
            			if(nodes[m][j].equals("0")){
            				bx = String.valueOf(j);
            				by = String.valueOf(m);
            			}
            			break;
            		}
            		
	            	String.format("%s %s %s %s %s %s", i, j, rx, ry, bx, by);
            	}
            }
            System.err.print("\n");
        }

        // Three coordinates: a node, its right neighbor, its bottom neighbor
        System.out.println("0 0 1 0 0 1");
        System.out.println("1 0 -1 -1 -1 -1");
        System.out.println("0 1 -1 -1 -1 -1");
    }
}