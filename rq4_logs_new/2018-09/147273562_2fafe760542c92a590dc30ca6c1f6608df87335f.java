import java.util.*;
public class Kmp
{
	public int lps[];
public void SubString(String t,String p,int n,int m)
{
	lps=new int[m];
	int i=1;
	lps[0]=0;
	int j=0;
	char c[]=p.toCharArray();
	char c1[]=t.toCharArray();
	while(i<m)
	{
		if(c[i]==c[j])
		{
			lps[i]=lps[i-1]+1;
			i++;
			j++;
		}
		else
		{
			if(j!=0)
			{
				j=lps[j-1];
			}
			else
			{
				lps[j]=0;
				i++;
			}
		}
	}
	i=0;
	j=0;
	while(i<n)
	{
		if(c1[i]==c[j])
		{
			i++;
			j++;
		}
		if(j==m)
		{
			System.out.println("Pattern found at  "+(i-j));
			j=lps[j-1];
		}
		else if(c1[i]!=c[j] && i<n)
		{
			if(j!=0)
			j=lps[j-1];
			else {
				lps[j]=0;
				i++;
		}
		}
	}

}

public static void main(String args[])
{
Kmp k=new Kmp();
k.SubString("abaabcccabc","abc",11,3);
//System.out.println("khn");
}
}