package uk.co.staticvoid.gliderrider.helper;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

public class BukkitHelper {

    public void broadcastMessage(String message) {
        Bukkit.getOnlinePlayers().forEach(p -> consoleNotification(p, message));
    }

    public void consoleNotification(Player player, String message) {
        player.sendMessage(message);
    }

    public void titleNotification(Player player, String title, String subtitle) {
        player.sendTitle(title, subtitle, 20, 100, 20 );
    }

}