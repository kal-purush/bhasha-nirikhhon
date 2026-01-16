package brodovi;

import java.util.Random;

public class FabrikaBrodova 
{
	public NeprijateljskiBrod KreirajRandomBrod()
	{
		NeprijateljskiBrod brod;
		
		Random random_genrator = new Random();
		int x =  random_genrator.nextInt(3);
		
		if(x == 0)
			brod = new Razarac();
		else if(x == 1)
			brod = new NosacAviona();
		else 
			brod = new Fregata();
		
		return brod;
	
	}
	
	public NeprijateljskiBrod KreirajBrod(String s)
	{
		NeprijateljskiBrod brod = null;
		
		
		
		if(s.equalsIgnoreCase("razarac"))
			brod = new Razarac();
		else if(s.equalsIgnoreCase("nosac"))
			brod= new NosacAviona();
		else if(s.equalsIgnoreCase("fregata"))
			brod = new Fregata();
			
		return brod;
	
	}
	
}