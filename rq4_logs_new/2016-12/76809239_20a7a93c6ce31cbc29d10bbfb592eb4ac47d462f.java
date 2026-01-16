package com.Stack;

import java.util.Stack;

public class EvaluateInfix {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			System.out.println(evaluate("100 * ( 2 + 12 ) / 14"));
	}

	
	public static int evaluate(String str)
	{
		Stack<Integer> values = new Stack<Integer>();
		Stack<Character> operator = new Stack<Character>();
		
		char[] arr = str.toCharArray();
		for(int i=0;i<arr.length;i++)
		{
			
			if(arr[i]==' ')
				continue;
			
			if(arr[i]=='(')
				operator.push(arr[i]);
			
			else if(arr[i] >= '0' && arr[i] <= 	'9')
			{
				StringBuffer sb=  new StringBuffer();
				while(i<arr.length && arr[i] >= '0' && arr[i] <= '9')
				{
					sb.append(arr[i++]);
					
				}
				values.push(Integer.parseInt(sb.toString()));
				
			}
			
			else if(arr[i]==')')
			{
				while(operator.peek()!='(')
				{
					values.push(applyOp(operator.pop(), values.pop(), values.pop()));
				}
				operator.pop();
			}
			
			else if(arr[i]=='+' || arr[i]=='-' || arr[i]=='*' || arr[i]=='/')
			{
				while(!operator.isEmpty() && hasPrecedence(operator.peek(), arr[i]))
				{
					values.push(applyOp(operator.pop(), values.pop(), values.pop()));
				}
				operator.push(arr[i]);
			}
		}
		
		//Now entire expression is parsed evaluate remaining tokens in the stacks
		while(!operator.empty())
		{
			values.push(applyOp(operator.pop(), values.pop(), values.pop()));
		}
		return values.pop();
	}
	
	public static boolean hasPrecedence(char a,char b)
	{
		if(a=='(' || b==')')
			return false;
		if((a == '+' || b == '-') && (a == '*' || b =='/'))
			return false;
		else
			return true;
			
	}
	
	public static int applyOp(char op,int a,int b)
	{
		switch(op)
		{
			case '+':
				return a+b;
			case '-':
				return a-b;
			case '*':
				return a*b;
			case '/':
				return a/b;
				
		}
		return 0;
	}
}