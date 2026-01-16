package maps;
import java.util.ArrayList;
import java.awt.Color;
import props.*;

public class ForestMapTest 
{
	public ArrayList<Building> buildings = new ArrayList<Building>();
	public ArrayList<Tree> trees = new ArrayList<Tree>();
	public ArrayList<River> rivers = new ArrayList<River>();
	public ArrayList<Bridge> bridges = new ArrayList<Bridge>();
	
	//Custom Colors:
	Color CabinRoof = new Color(4, 4, 112);
	
	public void addProps()
	{
		buildings.add(new Building(70, 50, 70, 70, CabinRoof));
		
		trees.add(new Tree(10, 150, 20));
		
		rivers.add(new River(200, 0, 25, 225));
		rivers.add(new River(0, 200, 225, 25));
		
		
		bridges.add(new Bridge(100, 195, 25, 35));
	}
}