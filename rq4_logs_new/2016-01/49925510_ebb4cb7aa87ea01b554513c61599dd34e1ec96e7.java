import java.util.*;
import java.io.*;
import java.math.*;

public class I {
	
	static Scanner in;
	static PrintStream out;
	
	static {
		/*if (System.getProperty("ONLINE_JUDGE") == null) {
      		in = new Scanner(System.in);
      		out = System.out;
    	} else */{
      		String file = "toral";
      		try {
        		in = new Scanner(new File(file + ".in"));
        		out = new PrintStream(new FileOutputStream(file + ".out"));
      		} catch (IOException ex) {
      		}
    	}
	}
	
	public static BigInteger gcd(BigInteger A,BigInteger B) {
		if (B.equals(BigInteger.ZERO)) return A;
		return gcd(B,A.mod(B));
	}
	
	public static void main(String[] args) {
		BigInteger A = in.nextBigInteger();
		BigInteger B = in.nextBigInteger();
		
		out.println(gcd(A,B));		
	}
}