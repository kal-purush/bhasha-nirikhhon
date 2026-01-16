package Base;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.html.parser.Entity;

public class Prog2 {
	
	public void m1() {
		// hashmap r=2,g=2, m=2
		String str = "programmingg";
		HashMap<Character, Integer> map = new HashMap<Character, Integer>();
		for(int i=0;i<str.length();i++)
		{
			char ch = str.charAt(i);
			map.put(ch, map.getOrDefault(ch, 0)+1);
		}
		for(Entry<Character, Integer> value: map.entrySet())
		{
//			if(value.getValue()>1)
				System.out.println(value.getKey() +" = " + value.getValue());
		}
		
	}
	public void m2()
	{
		String str = "programmingg";
		char ch [] = str.toCharArray(); int count;
		for(int i=0;i<ch.length;i++)
		{
			count=1;
			for(int j=i+1;j<ch.length;j++)
			{
				if(ch[i]==ch[j])
					count++;
			}
			if(count>1 && count<3)
			System.out.println(ch[i] +"=" + count);
		}
		
		
	}
	public void m3()
	{
		int arr[] = {2,5,3,7,6,3,6,8,9};int count;
		for(int j=0;j<arr.length;j++)
		{ 
			count=1;
			for(int i=j+1;i<arr.length;i++)
			{
				if(arr[j]==arr[i])
					count++;
			}
			if(count>1)
			System.out.println(arr[j] +" = " + count );
		}
		
		
	}
	
	public void m4() {
		
		String str = "123abcf@4";
//		Pattern p = Pattern.compile("[0-9]");
		Pattern p = Pattern.compile("[@]");
		Matcher m = p.matcher(str);
		while(m.find())
		{
			System.out.print(m.group() + " ");
		}
		
	}
	public void m5()
	{
		int count=0;
		int arr[] = {0,1,2,0,4,3,0,5,0};
		for(int i=0;i<arr.length;i++)
		{
			if(arr[i]==0)
				count++;
		}
		for(int i=0;i<arr.length;i++)
		{
			if(i<count)
				arr[i]=0;
			else
				arr[i]=1;
		}
		for(int a :arr)
		{
			System.out.print(a + " ");
		}
	}
	
	
	public static void main(String[] args) {
		Prog2 a = new Prog2();
		a.m5();
//		String str1 = "aniket";
//		String str2 = "aniket";
//		String str3 = new String("aniket");
//		String str4 = new String("aniket");
//		System.out.println(str1 == str2);
//		str2 =str2.toUpperCase();
//		System.out.println(str2 == str3);
//		System.out.println(str3==str4);
//		StringBuffer str5 = new StringBuffer("aniket");
//		StringBuffer str6 = new StringBuffer("aniket");
//		System.out.println(str5==str6);
		
		
	}

}