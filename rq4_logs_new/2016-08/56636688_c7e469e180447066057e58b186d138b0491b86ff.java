package level1;
import java.util.*;
import java.io.*;
import java.math.*;

class ChuckNorris {

    public static void main(String args[]) {
        Scanner in = new Scanner(System.in);
        String MESSAGE = in.nextLine();
        
        String boo = "";
        for(int i = 0; i < MESSAGE.length(); i++){
            char A = MESSAGE.charAt(i); 
            int ascii = A; 
            String k = Integer.toBinaryString(ascii);
            
            if(k.length() < 7){
        	    while(k.length() < 7){
        		    k = "0" + k;
        	    }
            }
            
            boo = boo + k;
        }
        
        if(boo.length() < 7){
        	while(boo.length() < 7){
        		boo = "0" + boo;
        	}
        }
          
        
        //counting number of bytes in each group
        int count = 1;
        
        for(int i = 0; i < boo.length()-1; i++){    	
        	
        	if(boo.charAt(i) == boo.charAt(i+1)){
        		count++;
        	}
        	
        	else  if(boo.charAt(i) != boo.charAt(i+1)){
        		if(boo.charAt(i) == '0'){
                    System.out.print("00 ");
                    while(count > 0){
                        System.out.print("0");
                        count--;
                    }
                    System.out.print(" ");
                }
                else if(boo.charAt(i) == '1'){
                    System.out.print("0 ");
                    while(count > 0){
                        System.out.print("0");
                        count--;
                    }
                    System.out.print(" ");
                }
        		count = 1;
        	}        	
        }
        
        //last group:
        if(boo.charAt(boo.length()-1) == '0'){
            System.out.print("00 ");
            while(count > 0){
                System.out.print("0");
                count--;
            }
        }
        else if(boo.charAt(boo.length()-1) == '1'){
            System.out.print("0 ");
            while(count > 0){
                System.out.print("0");
                count--;
            }
        }
    }
}