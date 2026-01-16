package data_structure;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EmptyStackException;
import java.util.Scanner;
import java.util.Stack;

public class BalancedTree {
	
	public static void main(String[] args) throws NumberFormatException, IOException ,EmptyStackException{
		
		
		Stack<Integer> stack = new Stack<Integer>();
//BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
	//	int n=Integer.parseInt(br.readLine());
		
		Scanner sc = new Scanner(System.in);
		String exp=sc.next();
		
		int length = exp.length();
		
	for(int i = 0; i<length ; i++)
	{
		char ch = exp.charAt(i);
		if(ch =='('||ch == '{'||ch == '[')
		{
			stack.push(i);
		}
		else if(ch == ')'||ch == '}'||ch == ']')
		{
			try
			{
			int p=(stack.pop()+1);
			System.out.println(ch+" at index "+(i+1)+" matched with"+ch+"at index "+p);	
			}
			
			catch(Exception e)
			{
				System.out.println("index not matched");
			}
			finally
			{
				while(!stack.isEmpty())
				{
					System.out.println(" at index "+(stack.pop() +1)+" is unmatched");
				}
			}
		}
	}
	
	
	
	//System.out.println(stack);


	}
	}