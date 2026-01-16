package level1;

import java.util.*;
import java.io.*;
import java.math.*;

class Defibrillators {
	
	 public static void main(String args[]) {
		double min = 20036; //longest distance on Earth
		String res = null;  
		 
        Scanner in = new Scanner(System.in);
        String LON = in.next();
        String LAT = in.next();
        double LONa = Double.valueOf(LON.replace(',', '.'));
        double LATa = Double.valueOf(LAT.replace(',', '.'));
        int N = in.nextInt();
        in.nextLine(); 
              
        for (int i = 0; i < N; i++) {
        	String input[] = new String[6];
            String DEFIB = in.nextLine();
            input = DEFIB.split(";");
           
            double LONb = Double.valueOf(input[4].replace(',', '.'));
            double LATb = Double.valueOf(input[5].replace(',', '.'));
            
            double x = (LONb - LONa)*Math.cos((LATa+LATb)/2);
            double y = (LATb - LATa);
            double d = Math.sqrt(Math.pow(x, 2)+ Math.pow(y, 2))*6371;
            
            if(d <= min){
            	min = d;
            	res = input[1];
            }
        }
        System.out.println(res);
    }	
}