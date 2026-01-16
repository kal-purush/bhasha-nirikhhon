/*
State A and State B are the initial States.
State D is the final state.
*/


import java.util.*;
import java.util.regex.*;
import java.io.*;
import javax.swing.*;
public class pathFinder {
	
	public static Boolean pres(List<String> st,String abc)
	{ 
		int count=0;
		
		for(int aa=0;aa<st.size();aa++)
		{
			if(st.get(aa) == abc )
				count++;
			
			//2 denotes that a path can have upto 2 times a single initial state. You can change this number if you want to have more or less than 2 times a single initial state
			if(count==2)	
				return true;
			
		}
		return false;
	}
	
	public static List<List<String>> recur(List<List<String>> path, List<String> st,List<List<String>> x)
	{
		//D because d is the final state
		if(st.get(st.size()-1)=="D")
		{	
			List<String> y = new LinkedList<String>();
			path.add(y);
			y.add(st.get(st.size() - 1));
			return path;
		}
		
		
		
		
		for(int i =0;i<x.size();i++)
		{
			if(st.size()==0)
				return path;
			if(x.get(i).get(0)==st.get(st.size() - 1))
			{
				for(int j =1;j<x.get(i).size();j++)
				{	
					//checks if the string is already present in the stack
					if(pres(st,x.get(i).get(j))==true)
						continue;
					
					//D because d is the final state
					if(x.get(i).get(j)=="D")
						{
							st.add(x.get(i).get(j));
							List<String> y = new LinkedList<String>();
							path.add(y);
								
							for(int lm =0;lm<st.size();lm++)
								y.add(st.get(lm));
								
							st.remove(st.size() - 1);
							continue;
						}
					else
					{
						st.add(x.get(i).get(j));
						path = recur(path,st,x);
					}	
				}
			}				
		}
		if(st.size()>0)
			st.remove(st.size() - 1);		

		return path;
	}
	
	public static void main( String[] args )
	{
		//List of list of string which stores where each state can reach
		List<List<String>> x = new ArrayList<List<String>>();
		
		for(int i=0;i<5;i++)
		{
			//make a new list of strings
			List<String> y = new LinkedList<String>();
			x.add(y);
		}

		//This means A can go to C,A and B
		x.get(0).add("A");
		x.get(0).add("C");
		x.get(0).add("A");
		x.get(0).add("B");
		
		//This means B can go to E,A and B
		x.get(1).add("B");
		x.get(1).add("E");
		x.get(1).add("A");
		x.get(1).add("B");
		
		//This means C can go to D,A and B
		x.get(2).add("C");
		x.get(2).add("D");
		x.get(2).add("A");
		x.get(2).add("B");
		
		//This means D can go to A and B
		x.get(3).add("D");
		x.get(3).add("A");
		x.get(3).add("B");
		
		//This means E can go to C,A and B
		x.get(4).add("E");
		x.get(4).add("C");
		x.get(4).add("A");
		x.get(4).add("B");
		
		//print function which shows where each state can reach
		for(int i =0;i<x.size();i++)
		{
			for(int j =0;j<x.get(i).size();j++)
				System.out.print(x.get(i).get(j)+" ----> ");
			System.out.println();
		}
		
		
		//list of list of strings to store the path
		List<List<String>> path=new ArrayList<List<String>>();
		
		//act as a stack
		List<String> st = new LinkedList<String>();
		
		//for loop conditions less than 2 because there are only 2 initial states
		for(int i=0;i<2;i++)
		{
			st.add(x.get(i).get(0));	
			path = recur(path,st,x);	
			st = new LinkedList<String>();		
		}
		
		//Variable used to display the path number
		int count=1;

		System.out.println();

		//print function that prints the path
		for(int i= 0;i<path.size();i++)
			{
				System.out.print("Path "+count++ +": " );
				for(int j= 0;j<path.get(i).size();j++)
					System.out.print( path.get(i).get(j)+" ");
				System.out.println();
			}		
	}

}