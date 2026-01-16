package Lobby;

import com.connorlinfoot.titleapi.TitleAPI;
import main.Arena;
import main.GameState;
import main.Nuke;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Material;
import org.bukkit.Sound;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.scheduler.BukkitRunnable;

import java.net.Inet4Address;

public class GameStart implements Listener {
    public static int counterBW;
    @EventHandler
    public void onStarterClick(PlayerInteractEvent e) {

        Player p = e.getPlayer();

        if(p.getItemInHand().getType() == Material.DIAMOND && p.hasPermission("Nuke.StartGame") && Arena.getState() == GameState.LOBBY) {

            startGame(10);

        }
    }

    public static void startGame(Integer secondsUntilStart) {
        counterBW = secondsUntilStart;

        new BukkitRunnable() {
            @Override
            public void run() {
                --counterBW;
                for (Player p : Bukkit.getOnlinePlayers()) {
                    p.sendMessage(Nuke.prefix + "§aDas Spiel startet in §5" + counterBW + " §aSekunden!");
                    p.playSound(p.getLocation(), Sound.ANVIL_LAND, 1L, 1L);
                    if(counterBW <= 5) {
                        TitleAPI.sendTitle(p, 3,  20, 3, ChatColor.YELLOW + "Start in: " + ChatColor.LIGHT_PURPLE + counterBW);
                    }
                    if(counterBW == 0) {
                        p.playSound(p.getLocation(), Sound.VILLAGER_YES, 1L, 1L);
                        cancel();
                        Arena.setState(GameState.GAME);
                    }
                }

            }
        }.runTaskTimer(Nuke.getPlugin(Nuke.class), 20, 20);
    }

}