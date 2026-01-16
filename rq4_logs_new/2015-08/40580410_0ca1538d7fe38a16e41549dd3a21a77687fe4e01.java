package algorithms.extsort;


public class UTF8 implements Comparable<UTF8>
{
	//���UTF8����
	private String utf8code;

	private int[] arrayIntUTF;
	UTF8(String str)
	{
		this.utf8code = utf8ToUnicode(str);
		System.out.println(this.utf8code);
	}
	
	public static String utf8ToUnicode(String inStr)
	{
		char[] myBuffer = inStr.toCharArray();
		for (char c : myBuffer)
		{
			//System.out.println((int)c);
		}
		
		StringBuffer sb = new StringBuffer();
		for (char c : myBuffer)
		{
			String hexS = Integer.toHexString(c);
	
			String unicode = "\\u" + hexS;
			sb.append(unicode.toLowerCase());
		}
		return sb.toString();
	}
	@Override
	public int compareTo(UTF8 o)
	{
		
		UTF8 other = (UTF8) o;
		if (other == this)
			return 0;
		if((this != null) && (o != null) && (this.utf8code !=null) && (o.utf8code !=null)  )
		{
			System.out.println(this.utf8code);
			System.out.println(this.utf8code);			
		}
		else
		{
			System.out.println(" UTF8 compareto ERROR");
			return 0;
		}

		return 0;
	}
	
	public String toString()
	{
		return this.utf8code;
	}
	
	
	public static void main(String args[])
	{ 
		UTF8 u0 = new UTF8("�������");
		UTF8 u1 = new UTF8("������");
	
		
	}
	
	
	
}