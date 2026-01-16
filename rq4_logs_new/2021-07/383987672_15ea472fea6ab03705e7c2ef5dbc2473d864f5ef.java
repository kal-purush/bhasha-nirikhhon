import java.util.*;
class Anagram
{
	public static void main(String[] args) 
	{
		Scanner scan=new Scanner();
		String s1=scan.nextLine();
		String s2=scan.nextLine();
		s1=s1.replaceAll(" ","");
		s2=s2.replaceAll(" ","");
		if(s1.length()==s2.length())
		{
			s1=s1.toupperCase();
			s2=s2.toupperCase();
			char ar1[]=s1.tocharArray();
			char ar2[]=s2.tocharArray();
			Arrays.sort(ar1);
			Arrays.sort(ar2);
			s1=new String(ar1);
			s2=new String(ar2);
			if(s1.equals(s2))
			{
				System.out.println("Anagram");
			}
			else
			{
				System.out.println("not Anagram");
			}
		}
		System.out.println(s1);
		System.out.println(s2);
	}
}
