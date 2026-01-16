package Compete;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class FibonacciWords {
	 public static void main(String[] args) throws IOException{
	        InputStreamReader read = new InputStreamReader(System.in);
	        BufferedReader reader = new BufferedReader(read);
	        int q = Integer.parseInt(reader.readLine());
	        while(q>0) {
	        	String line = reader.readLine();
	        	String[] data = line.split(" ");
	        	String one = data[0];
		        String two = data[1];
		        BigInteger n = new BigInteger(data[2]);
		        System.out.println(getResult(one, two, n));
	        	q--;
	        }
	        
	    }
	    static int getResult(String one, String two, BigInteger n) {
	        List<BigInteger> fcount = new ArrayList<BigInteger>();
	        BigInteger oneInt = BigInteger.valueOf(one.length());
	        BigInteger twoInt = BigInteger.valueOf(two.length());
	        fcount.add(oneInt);
	        fcount.add(twoInt);
	        int result = -1;
	        do{
	            fcount.add(oneInt.add(twoInt));
	            oneInt = twoInt;
	            twoInt = fcount.get(fcount.size()-1);
	        }while(twoInt.compareTo(n)<0);
	        int i = fcount.size() -3;
	        int currentIndex = i;
	        while(i>=0) {
	            BigInteger current = fcount.get(i);
	            if(n.compareTo(current)>0) {
	                n = n.subtract(current);
	                i--;
	                currentIndex = i;
	            }
	            else
	            {
	                if(i==0)
	                    result = one.charAt(n.intValue() - 1);
	                if(i==1) 
	                    result = two.charAt(n.intValue() - 1);
	                i = currentIndex - 2;
	                currentIndex = i;
	            }
	        }
	        if(result == -1)
	            result = two.charAt(n.intValue() - 1);
	        return (result - 48);
	        }
}