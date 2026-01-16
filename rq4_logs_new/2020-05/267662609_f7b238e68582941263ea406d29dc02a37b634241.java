package de.schlauhund.challenges;

import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World.Environment;
import org.bukkit.WorldCreator;
import org.bukkit.WorldType;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;

import de.schlauhund.utils.ItemCreator;

public class MLGChallenge {

	public static HashMap<UUID, Inventory> inventory = new HashMap<>();
	public static HashMap<UUID, Location> location = new HashMap<>();
	static ItemCreator ic = new ItemCreator();

	public static void startMLG(Player p) {
		Random random = new Random();
		double x = random.nextInt(100);
		double y = 100;
		double z = random.nextInt(100);

		inventory.put(p.getUniqueId(), p.getInventory());
		location.put(p.getUniqueId(), p.getLocation());
		p.getInventory().clear();
		p.getInventory().setItemInMainHand(ic.createItem(Material.WATER_BUCKET, "Wasser Eimer"));
		Location loc = new Location(Bukkit.getWorld("mlg"), x, y, z);
		p.teleport(loc);
	}

	public static void checkWorld() {
		WorldCreator wc = new WorldCreator("mlg");
		wc.environment(Environment.NORMAL);
		wc.type(WorldType.FLAT);
		wc.generateStructures(false);
		wc.createWorld();
	}
}