package com.studyguide.excercise;

public class SortIntegers {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int a;
		String name = "smit";
		try{
		a = Integer.parseInt(args[0]);
		int b = Integer.parseInt(args[1]);
		int c = Integer.parseInt(args[2]);
		final int[] num = {a,b,c};
		sort(num);
		for(int i=0; i<num.length;i++)
		{
			System.out.println(num[i]);
		}}
		catch(Exception ex){ System.out.println("Wrong output, get lost");}
		finally{ System.out.println("You're done");}
	a = 0 ;
	name = "xexes";
	switch(a)
	{
	case 0: System.out.println(10); break;
	case 2: System.out.println(100); break; 
	default: System.out.println("default");break;
	}
	
	}
	
	public static void sort(int a[])
	{
		for(int i=0; i<a.length;i++)
		{
			for(int j=i; j<a.length;j++)
			{
				if(a[i] > a[j])
				{
					int t = a[i];
					a[i] = a[j];
					a[j] = t;
				}
			}
		}
	}

}