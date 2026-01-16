import java.util.Scanner;

public class Change {
    private static int getChange(int m) {
	    int count=0;
	    int minCoin=m;
	    int answer=0;
	    
	    
	    while(minCoin>=10)
	    {
		    count=m/10;
		    minCoin=m%10;
	    }
	    while(minCoin>=5)
	    {
		    
		    minCoin=minCoin%5;
		    count++;
	    }
	    while(minCoin>0)
	    {
		    minCoin--;
		    count++;
	    }
	    answer=count;

        //write your code here
        return answer;
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int m = scanner.nextInt();
        System.out.println(getChange(m));

    }
}
