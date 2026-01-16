package assignment;

public class BinaryToDecimal {
	public static void main(String[] args) {
		long num=1010,dec=0;
	    dec=calculateDecimal(num);
	    System.out.println("Dec = "+dec);
	}

	private static long calculateDecimal(long num) {
		long i,temp=0,count=0;
		for(i=num;i>0;i/=10,count++){
			temp+=(i%10)*Math.pow(2, count);
		} 
		return temp;
	}     
			
}
