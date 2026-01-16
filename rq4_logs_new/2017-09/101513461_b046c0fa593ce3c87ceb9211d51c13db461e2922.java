package mr_krab.customjoinstream.events;

import org.bukkit.Effect;
import org.bukkit.Location;
import org.bukkit.Sound;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Player;

import mr_krab.customjoinstream.Plugin;

public class EventUtils {
	
	static Plugin instance;
	public EventUtils(Plugin plugin) {
		instance = plugin;
	}
	// Воспроизведение звука из конфига
	public static void playJoinSound(Player player) {
		String group = Plugin.getInstance().getChat().getPrimaryGroup(player);
        if(Plugin.getInstance().getConfig().get("Groups." + group) !=null) {
		        if(!player.hasPermission("customjoinstream.sound")) {
		            return;
		        }
	            player.playSound(player.getLocation(), Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound1").toUpperCase()), 10, 100);
	            player.playSound(player.getLocation(), Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound2").toUpperCase()), 10, 100);
		        for(Entity near : player.getNearbyEntities(Plugin.getInstance().getConfig().getDouble("Radius"),
		                Plugin.getInstance().getConfig().getDouble("Radius"),
		                Plugin.getInstance().getConfig().getDouble("Radius"))) {
		            if(!(near instanceof Player)) continue;
		            ((Player)near).playSound(player.getLocation(), Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound1").toUpperCase()), 10, 100);
		            ((Player)near).playSound(player.getLocation(), Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound2").toUpperCase()), 10, 100);
		        }
		        return;
		        }
		    }

	// Воспроизведение эффекта из конфига
	public static void playJoinEffect(Player player) {
		        String group = Plugin.getInstance().getChat().getPrimaryGroup(player);
		        if(Plugin.getInstance().getConfig().get("Groups." + group) !=null) {
		        if(!player.hasPermission("customjoinstream.effect")) {
		            return;
		        }
	            player.playEffect(player.getLocation(), Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect1")), null);
	            player.playEffect(player.getLocation(), Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect2")), null);
		        for(Entity near : player.getNearbyEntities(Plugin.getInstance().getConfig().getDouble("Radius"),
		                Plugin.getInstance().getConfig().getDouble("Radius"),
		                Plugin.getInstance().getConfig().getDouble("Radius"))) {
		            if(!(near instanceof Player)) continue;
		            ((Player)near).playEffect(player.getLocation(), Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect1")), null);
		            ((Player)near).playEffect(player.getLocation(), Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect2")), null);
		        }
		        return;
		        }
		    }
	public static void playerJoinTp (Player player) {
		if(Plugin.getInstance().getConfig().getBoolean("TpToStartLoc.Join")) {
			Location join = Plugin.getInstance().locm.TpJoinLoc(player.getName());
			player.teleport(join);
		}
		return;
	}
	public static void playerQuitTp (Player player) {
		if(Plugin.getInstance().getConfig().getBoolean("TpToStartLoc.Quit")) {
			Location join = Plugin.getInstance().locm.TpJoinLoc(player.getName());
			player.teleport(join);
		}
		return;
	}
}