package Problems;

public class BinaryGap {

	public static void main(String[] args) {
		int a=3;
		binaryGap(a);

	}
	  static public void binaryGap(int N) {
	        String bin=Integer.toBinaryString(N);
	        System.out.println(bin);
	        int length=0;
	        int j=0;
	        boolean first=true;
	        int templength=0;
	        for(int i=0;i<bin.length();i++)
	        {
	            
	            if(bin.charAt(i)==1) 
	            {
	            	i++;
	            }
	            if(bin.charAt(i)=='1')
	            {
	                templength=i-j;
	                j=i;
	            }
	            
	            if(templength>length)
	            {
	                length=templength;
	            }
	            if(first && bin.charAt(0)=='0')
	            {
	                length=0;
	                first=false;
	            }
	            
	        }
	        System.out.println(length);
	    }

}