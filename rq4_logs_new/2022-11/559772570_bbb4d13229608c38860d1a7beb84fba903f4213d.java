package ArraylistPractise;

import java.util.HashMap;

public class HashMapDemo1 
{

	public static void main(String[] args) 
	{
		//addElements();
		//readRemoveElements();
		ReadElements();

	}
	private static void addElements()
	{
		HashMap<String,String> obj=new HashMap<String,String>();
		System.out.println("Elements:"+obj);
		obj.put("mango", "mango is the friuts of king");
		obj.put("lotus", "lotus is the national flower");
		obj.put("banglore", "banglore is the gardencity");
		obj.put(null, "null key supported by hashmap");
		System.out.println("elemnets:"+obj);
		
	}
	private static void readRemoveElements()
	{
		HashMap<String,String> obj1=new HashMap<String,String>();
		obj1.put("mango", "mango is the fruits of king");
		obj1.put("lotus", "lotus is the national flower");
		obj1.put("banglore", "banglore is the gardencity");
		System.out.println("elements:"+obj1);
		//read elements
		String val=obj1.get("lotus");
		System.out.println("elements:"+obj1);
		//remove elements 
		obj1.remove("mango");
		System.out.println("elements:"+obj1);
	}
	private static void ReadElements()
	{
		HashMap<String,String> obj2=new HashMap<String,String>();
		System.out.println("elements:"+obj2);
		obj2.put("mango", "mango is the king of fruits");
		obj2.put("lotus", "lotus is the national flower");
		obj2.put("banglore", "banglore is the gardencity");
		System.out.println("elements:"+obj2);
		obj2.forEach((k,v) -> System.out.println(k +"===> "+v));
			
	}
}