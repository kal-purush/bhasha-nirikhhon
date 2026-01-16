import java.io.*;

public class SubarrayWithGivenSum {
	
	static void fun(int a[],int ts){
        int sum=0,start=1,end=0,j=0;
        for(int i=0;i<a.length;i++){
            
            
                sum+=a[i];
                end++;
            
            while(sum>ts){
                
                sum-=a[j];j++;
                start++;
            }
            if(sum==ts){
                System.out.println(start+" "+end);
                return;
            }
        }
        System.out.println("-1");
    }

	public static void main(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		BufferedReader ip= new BufferedReader(new InputStreamReader(System.in));
		
		int t = Integer.parseInt(ip.readLine());
		for(int j=0;j<t;j++){
		    String inp[]=new String[5];
           inp=ip.readLine().split(" ");
		    int n = Integer.parseInt(inp[0]);
		    String inp1[]=new String[n];
		    int ts = Integer.parseInt(inp[1]);
		    int a[] = new int[n];
		    inp1=ip.readLine().split(" ");
		    for(int i=0;i<n;i++)
		    {
		        
		        a[i]=Integer.parseInt(inp1[i]);
		    }
		     fun(a,ts);
		     
		}
	}
}