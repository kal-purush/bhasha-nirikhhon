
public class LeastNumber 
{

	
	static int reduce(int n,int k){
	
		if(k<=0){
		 return n;
		}
		if(n==0)
		{
		  return 10;
		}
		int path1=reduce(n/10,k)*10+n%10;
		int path2=reduce(n/10,k-1);
		return path1<path2?path1:path2;
	}
       public static void main(String[] args)
		{
		System.out.println("%u\n"+reduce(246,2));
		System.out.println("%u\n"+reduce(24635,3));
		System.out.println("%u\n"+reduce(53642,3));
		System.out.println("%u\n"+reduce(21,1));
		}


}