import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class Dowhile {

	public static void main(String[] args) throws IOException {
		int n,c=0;
	      System.out.println("Enter an integer");
	      BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
	  	  n=Integer.parseInt(br.readLine());
	      
	      do{
	    	
	    	  System.out.println(" ");
	    	  System.out.println(n+"*"+c+" = "+(n*c));
	    	  c++;
	      }
	      while (c < 10);
	      
	}

}