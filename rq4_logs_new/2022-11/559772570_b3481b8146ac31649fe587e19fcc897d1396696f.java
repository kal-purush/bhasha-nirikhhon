package arraylistpractice;

import java.util.HashSet;

class B
{
	static HashSet<String> aa1(String a,HashSet<String>hset,String action)
	{
		switch(action)
		{
		case "add":
			hset.add(a);
			break;
		case "remove":
			hset.remove(a);
			break;
		}
		return hset;
	}
}
class A
{
	static HashSet<String> aa()
	{
		HashSet<String> obj=new HashSet<String>();
		obj.add("a");
		obj.add("aa");
		obj.add("1");
		return obj;
	}
}

public class hashsetdemo1 
{

	public static void main(String[] args) 
	{
		HashSet<String>hset=new HashSet<String>();
		B.aa1("apple", hset, "add");
		B.aa1("banana", hset, "add");
		B.aa1("pineapple", hset, "add");
		B.aa1("greenApple", hset, "add");
		System.out.println(hset);
		hset=B.aa1("greenApple", hset, "remove");
		System.out.println(hset);
		int a[]= {1,4,5,6,3,1,3};

	}

}