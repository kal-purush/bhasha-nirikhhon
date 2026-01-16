import java.util.Stack;

public class BalanceParens
{
	public static String parens = "([{<)]}>";
	
	public static boolean isBalanced(String str) 
	{
		Stack<Integer> stack = new Stack<Integer>();
		for(int i = 0; i < str.length(); i++) 
		{
			int type = parens.indexOf(str.charAt(i));
			if(type == -1)
				continue;
			
			if(type < 3)
				stack.push(type);
			else if(stack.pop() != type - 4)
				return false;
		}
		return stack.size() == 0;
	}
	
	public static void main(String[] args) 
	{
		// Some Tests
		String test1 = "[()]";
		System.out.printf("Test 1, %s: %b\n",test1,isBalanced(test1));
		
		String test2 = "Hello [This is a nest {eyyo} ( This left paren is illegal]";
		System.out.printf("Test 2, %s: %b\n",test2,isBalanced(test2));
		
		String test3 = "{{{{}}}";
		System.out.printf("Test 3, %s: %b\n",test3,isBalanced(test3));
		
		String test4 = "{[{[(Stuff) Things] Other} Stuff] More}";
		System.out.printf("Test 4, %s: %b\n",test4,isBalanced(test4));
	}
}