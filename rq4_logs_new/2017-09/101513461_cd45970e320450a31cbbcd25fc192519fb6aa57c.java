package mr_krab.customjoinstream.utils;

import java.util.List;
import java.util.stream.Collectors;

import org.bukkit.Bukkit;
import org.bukkit.Effect;
import org.bukkit.Location;
import org.bukkit.Sound;
import org.bukkit.entity.Player;

import mr_krab.customjoinstream.Plugin;

public class EventUtils {
	
	static Plugin instance;
	public EventUtils(Plugin plugin) {
		instance = plugin;
	}
		// Воспроизведение звука из конфига
	public static void playJoinSound(Player player) {
		 if(!player.hasPermission("customjoinstream.sound")) {
	            return;
	        }
	        String group = Plugin.getInstance().getChat().getPrimaryGroup(player);
            player.playSound(player.getLocation(), Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound1").toUpperCase()), 10, 100);
            player.playSound(player.getLocation(), Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound2").toUpperCase()), 10, 100);
	        getNearbyPlayers(player).forEach(near -> {
	            near.playSound(player.getLocation(),
	                    Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound1").toUpperCase()), 10, 100);
	            near.playSound(player.getLocation(),
	                    Sound.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Sounds.Sound2").toUpperCase()), 10, 100);
	        });
	    }


		// Воспроизведение эффекта из конфига
	public static void playJoinEffect(Player player) {
		if(!player.hasPermission("customjoinstream.effect")) {
            return;
        }
        String group = Plugin.getInstance().getChat().getPrimaryGroup(player);
        player.playEffect(player.getLocation(), Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect1")), null);
        player.playEffect(player.getLocation(), Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect2")), null);
        getNearbyPlayers(player).forEach(near -> {
            near.playEffect(player.getLocation(),
                    Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect1")), null);
            near.playEffect(player.getLocation(),
                    Effect.valueOf(Plugin.getInstance().getConfig().getString("Groups." + group + ".Effects.Effect2")), null);
        });
		}
		// Радиус действия эффекта из конфига
    private static List<Player> getNearbyPlayers(Player player) {
        return player.getNearbyEntities(Plugin.getInstance().getConfig().getDouble("Radius"),
                Plugin.getInstance().getConfig().getDouble("Radius"),
                Plugin.getInstance().getConfig().getDouble("Radius")).stream()
                .filter(entity -> entity instanceof Player)
                .map(entity -> (Player)entity).collect(Collectors.toList());
    }
		// Телепортация игрока при входе
	public static void playerJoinTp (Player player) {
		if(Plugin.getInstance().getConfig().getBoolean("TpToStartLoc.Join")) {
				Location join = Plugin.getInstance().locm.TpJoinLoc(player.getName());
				player.teleport(join);
		}
	}
	
		// Телепортация игрока при выходе
	public static void playerQuitTp (Player player) {
		if(Plugin.getInstance().getConfig().getBoolean("TpToStartLoc.Quit")) {
				Location join = Plugin.getInstance().locm.TpJoinLoc(player.getName());
				player.teleport(join);
		}
	}
	
	// Скрытие игрока
	public static void playerHide (Player player) {
		if(player.hasPermission("customjoinstream.hide")) {
		Bukkit.getOnlinePlayers().forEach(p -> {
            if(!(p.equals(player))){
            	p.hidePlayer(player);
            	}
        });
		}
	}
	
	// Отображение игрока
	public static void playerShow (Player player) {
		if(player.hasPermission("customjoinstream.hide")) {
		Bukkit.getOnlinePlayers().forEach(p -> {
            if(!(p.equals(player))){
            	p.showPlayer(player);
            	}
        });
		}
	}
	
	// Скрытие всех игроков
	public static void hideAllPlayers (Player player) {
		if(Plugin.getInstance().getConfig().getBoolean("HideAllPlayers")) {
		Bukkit.getOnlinePlayers().forEach(p -> {
            if(!(p.equals(player))){
            	p.hidePlayer(player);
            	player.hidePlayer(p);
            }
        });
		}
	}
	
}