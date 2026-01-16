package es.eltrueno.deliveryman.utils;

import java.util.ArrayList;

import org.bukkit.Location;
import org.bukkit.entity.Player;

public class Example {
	
	
	public static void NormalHologram(Location loc, ArrayList<String> lines){
		//Create a new hologram visible to all the players.
		TruenoHologram hologram = new  TruenoHologram(loc, lines);
		
		//Set distance between hologram lines. Default: 0.28
		hologram.setDistanceBetweenLines(0.28);
		
		//Display the hologram
		hologram.Display();
		
		//Update a specific line
		hologram.UpdateLine(0, "text");
		
		//Delete a specific line
		hologram.RemoveLine(0);
		
		//Delete the hologram
		hologram.delete();
	}
	
	public static void PlayerHologram(Player p, Location loc, ArrayList<String> lines){
		//Create a new hologram visible to a specific player.
		TruenoHologram hologram = new  TruenoHologram(p, loc, lines);
		
		//Set distance between hologram lines. Default: 0.28
		hologram.setDistanceBetweenLines(0.28);
		
		//Display the hologram
		hologram.Display();
		
		//Update a specific line
		hologram.UpdateLine(0, "text");
		
		//Delete a specific line
		hologram.RemoveLine(0);
		
		//Delete the hologram
		hologram.delete();
	}
		
}