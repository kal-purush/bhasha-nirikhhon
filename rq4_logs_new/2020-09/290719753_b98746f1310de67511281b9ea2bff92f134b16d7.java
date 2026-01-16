package MultiThreading;

public class Alphabet extends Thread{

	public void run()
	{
		for(char c='A';c<='Z';c++)
		{
			System.out.print(" "+c);
			
		}
	}
	
	
	
	
}