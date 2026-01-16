package Game;

import java.util.ArrayList;

import info.gridworld.actor.Actor;
import info.gridworld.grid.Location;

public class Bishop extends Actor{

	
	
	
	
	public void act()
	{
		
	}
	
	public boolean getC()
	{
		return true;
	}
	
	public ArrayList<Location> getmoveLocations()
	{
		ArrayList<Location> ans = new ArrayList<Location>();
		
		
		
		for(int i = 1; i<=7;i++)
		{
			Location x = new Location(getLocation().getRow()+i,getLocation().getCol()+i);
			if(getGrid().isValid(x))
			{
				if(!getGrid().getOccupiedLocations().contains(x))
				{
					ans.add(x);
				}
				if(getGrid().getOccupiedLocations().contains(x) && !(getGrid().get(x).getC()== getC()))
				{
					ans.add(x);
					i = 8;
				}
				
				else if(getGrid().getOccupiedLocations().contains(x) && (getGrid().get(x).getC()== getC()))
				{
					
					i = 8;
				}
			}
			
		}
		
		for(int i = 1; i<=7;i++)
		{
			Location x = new Location(getLocation().getRow()-i,getLocation().getCol()-i);
			if(getGrid().isValid(x))
			{
				if(!getGrid().getOccupiedLocations().contains(x))
				{
					ans.add(x);
				}
				if(getGrid().getOccupiedLocations().contains(x) && !(getGrid().get(x).getC()== getC()))
				{
					ans.add(x);
					i = 8;
				}
				
				else if(getGrid().getOccupiedLocations().contains(x) && (getGrid().get(x).getC()== getC()))
				{
					
					i = 8;
				}
			}
		}
		
		for(int i = 1; i<=7;i++)
		{
			Location x = new Location(getLocation().getRow()-i,getLocation().getCol()+i);
			
			if(getGrid().isValid(x))
			{
				if(!getGrid().getOccupiedLocations().contains(x))
				{
					ans.add(x);
				}
				if(getGrid().getOccupiedLocations().contains(x) && !(getGrid().get(x).getC()== getC()))
				{
					ans.add(x);
					i = 8;
				}
				
				else if(getGrid().getOccupiedLocations().contains(x) && (getGrid().get(x).getC()== getC()))
				{
					
					i = 8;
				}
			}
		}
		
		for(int i = 1; i<=7;i++)
		{
			Location x = new Location(getLocation().getRow()+i,getLocation().getCol()-i);
			if(getGrid().isValid(x))
			{
				if(!getGrid().getOccupiedLocations().contains(x))
				{
					ans.add(x);
				}
				if(getGrid().getOccupiedLocations().contains(x) && !(getGrid().get(x).getC()== getC()))
				{
					ans.add(x);
					i = 8;
				}
				
				else if(getGrid().getOccupiedLocations().contains(x) && (getGrid().get(x).getC()== getC()))
				{
					
					i = 8;
				}
			}
		}
		return ans;
		
		
	}
	
	public String toString()
	{
		return "Bishop";
	}
	
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub

	}

}