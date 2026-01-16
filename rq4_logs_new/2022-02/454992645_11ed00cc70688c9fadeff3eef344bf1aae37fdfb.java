package edu.ufl.cise.plc;

import edu.ufl.cise.plc.IToken.Kind;
import java.util.ArrayList;

public class Lexer implements ILexer{
	private enum State
	{
		START,
		IN_IDENT,
		HAVE_ZERO,
		HAVE_DOT,
		IN_FLOAT,
		IN_NUM,
		HAVE_EQ,
		HAVE_MINUS
	}
	
	public Lexer(String input)
	{
		int pos = 0;
		int startPos = 0;
		int len = 0;
		State state = State.START;
		while (true)
		{
			char[] chars = input.toCharArray();
			char ch = chars [pos];
			if (state == State.START) //START CASE
			{
				startPos = pos;
				switch(ch) //issue: if there is no 0, then we are stuck?
				{
					case ' ', '\t', '\n', '\r' -> 
					{
						pos++;
					}
					case '+' -> 
					{
						Token newToken = new Token(Kind.PLUS, Character.toString(ch) ,startPos, 1); //WHERE AM I STORING THE TOKENS??
						pos++;
					}
					case '*' -> 
					{
						Token newToken = new Token(Kind.TIMES, Character.toString(ch) ,startPos, 1); 
						pos++;
					}
					case '=' ->
					{
						state = State.HAVE_EQ; 
						pos++;
					}
					case 0 ->
					{
						// instructions say to add an EOF token?
						return;
					}
				
				}
				
			}
			else if (state == State.HAVE_EQ)
			{
				switch (ch)
				{
					case '=' ->
					{
						Token newToken = new Token(Kind.EQUALS, Character.toString(ch), startPos, 2);
						pos++;
					}
					default->
					{
						//throw an exception or something?
						return;
					}
				}
			}
			else if (state == State.IN_NUM)
			{
				int tokenPos = 0;
				ArrayList<Character> numbers = new ArrayList<Character>(); //store the numbers as we go along in this character array
				switch(ch)
				{
					
					case '0','1','2','3','4','5','6','7','8','9' ->
					{
						tokenPos++;
						pos++;
						numbers.add(ch);
					}
					default ->
					{
						Token newToken = new Token(Kind.INT_LIT, numbers.toString(), tokenPos, pos-tokenPos);
						state = State.START;
					}
				}
				
				
			}
		}
	}
	

}