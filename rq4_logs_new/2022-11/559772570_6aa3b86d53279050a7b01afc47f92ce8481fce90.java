package ArraylistPractise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Arraylist1 
{

	public static void main(String[] args) 
	{
		addElements();
		removeElements();
		readElements1();
		readelments2();
		readElements3();
		withoutgenerics();

	}
	private static void addElements()
	{
		List<Integer> obj=new ArrayList<Integer>();
		System.out.println("Elements:"+obj);
		obj.add(100);
		obj.add(200);
		obj.add(300);
		obj.add(400);
		System.out.println("Elements:"+obj);
		List<Integer> obj1=new ArrayList<Integer>();
		obj1.add(111);
		obj1.add(222);
		obj1.add(333);
		obj1.add(444);
		obj.addAll(obj1);
		System.out.println("Elements:"+obj);	
	}
	private static void removeElements()
	{
		List<Integer> obj=new ArrayList<Integer>();
		System.out.println("elements:"+obj);
		obj.add(100);
		obj.add(200);
		obj.add(300);
		obj.add(400);
		System.out.println("elements:"+obj);
		obj.remove(1);
		System.out.println("elements:"+obj);
		obj.remove(Integer.valueOf(100));
		System.out.println("elements:"+obj);
		obj.removeAll(obj);
		System.out.println("elements:"+obj);
	}


	private static void readElements1()
	{
		List<Integer> obj=new ArrayList<Integer>();
		System.out.println("elements:"+obj);
		obj.add(100);
		obj.add(200);
		obj.add(300);
		obj.add(400);
		System.out.println("elements:"+obj);
		for(int a:obj);
		{
			System.out.println('a');
		}
	}



	private static void readelments2()
	{
		List<Integer> obj=new ArrayList<Integer>();
		System.out.println("elements:"+obj);
		obj.add(100);
		obj.add(200);
		obj.add(300);
		obj.add(400);
		System.out.println("elements:"+obj);
		for(int i=0;i<obj.size();i++)
		{
			System.out.println(i);
		}
	}

	private static void readElements3()
	{
		List<Integer> obj=new ArrayList<Integer>();
		System.out.println("elements:"+obj);
		obj.add(100);
		obj.add(200);
		obj.add(300);
		obj.add(400);
		System.out.println("elements:"+obj);
		Iterator<Integer> ite=obj.iterator();
		while(ite.hasNext())
		{
			System.out.println(ite.hasNext());
		}
	}

	private static void withoutgenerics()
	{
		ArrayList obj=new ArrayList();
		System.out.println("Elements:"+obj);
		obj.add(100);
		obj.add("mango");
		obj.add("apple");
		obj.add(12.75);
		System.out.println("Elements:"+obj);
	}

}