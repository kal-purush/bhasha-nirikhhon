import java.io.*;
class Game
{
	int[] location;
	int noh;
	
	public String checkYourself(String ug)   // why public....well does not matter bcoz prog runs fin without public
	{
		String result="miss";
		int iug=Integer.parseInt(ug);
		for(int i:location)
		{
			if(iug==i)
			{
				result="hit";
				noh++;
				break;
			}
		}
		if(noh==location.length)//also try 3 & location.length()....after compilation error is produced if length() is used
			result="kill";
		System.out.println(result);
			return result;
			
	}
	void setLocation(int[] locs)
	{
		location=locs;
	}
}
class GameDriver
{
	public static void main(String args[])  //also try string args[].....runs fin
	{
		GameHelper helper= new GameHelper();
		int nog=0;//if i not initialise it compilation error is there: variable nog might not have been initialized .....why????
		Game g=new Game();
		int r=(int)(Math.random()*5);
		int[] ra={r,r+1,r+2};
		g.setLocation(ra);
		boolean gir=true;
		while(gir)
		{
			String guess=helper.getUserInput("enter the no"); // in book it is string guess ....i dont think it would create such a difference....f**k there is a difference b/w string[] and string...comile kara ke dekh le bhai
			String result=g.checkYourself(guess);
			nog++;
			if(result=="kill")
			{
				gir=false;
				
			}
			
		}
		System.out.println("you take "+nog+" no of guesses");
		
	}
}
//import java.io.*;
 class GameHelper
{
	public String getUserInput(String prompt)
	{
		String inputLine=null; //so in java NULL=null or wtf
		System.out.print(prompt+"   ");
		try
		{
		BufferedReader is = new BufferedReader(
		new InputStreamReader(System.in));
			inputLine=is.readLine();
			if(inputLine.length()==0)//now he is using length()
				return null;
		}
			catch(IOException e)
			{
				System.out.println("IOException:  "+e);
			}
		
		return inputLine;
	}
	}
	