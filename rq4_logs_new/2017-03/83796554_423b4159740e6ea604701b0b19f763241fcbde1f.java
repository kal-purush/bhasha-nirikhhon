import java.util.*;

public class DifferentSummands {
    private static List<Integer> optimalSummands(int n) {
        List<Integer> summands = new ArrayList<Integer>();
        int sum=0;
        int j=0;
        int secondlast,last;
        for (int i = 1; i <= n; i++) {
        	sum=sum+i;
        	summands.add(i);
        	j++;
        	if(sum>n)
        	{
        		secondlast=summands.get(j-2);
        		last=summands.get(j-1);
        		summands.remove(j-2);
        		j--;
        		summands.remove(j-1);
        		j--;
        	
        		sum=sum-secondlast;
        		summands.add(i);
        		j++;
        	}
        	if(sum==n)
        		break;
			
		}

        
        return summands;
    }
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        List<Integer> summands = optimalSummands(n);
        System.out.println(summands.size());
        for (Integer summand : summands) {
            System.out.print(summand + " ");
        }
    }
}
