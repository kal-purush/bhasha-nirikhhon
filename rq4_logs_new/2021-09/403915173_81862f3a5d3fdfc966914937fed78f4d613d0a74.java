public class Tech
{
	public static void main(String[]args)
	{
		int num=2025;
		String s=String.valueOf(num);
		StringBuffer sb=new StringBuffer(s);
        String sb1=sb.substring(2);
		String sb2=sb.substring(0,2);
		String sb3=sb2+sb1;
		Integer i=Integer.parseInt(sb3);
		int square=i*i;
		if(i==num)
		{
			System.out.println("The given number is Tech number");
		}
		else
		{
			System.out.println("The given number is not a Tech number");
		}
	}
}