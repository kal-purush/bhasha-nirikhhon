package xyz.sethy.vpn;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;
import xyz.sethy.vpn.events.VPNKickEvent;

/**
 * Created by seth on 02/09/16.
 */
public class Main extends JavaPlugin implements Listener
{
    public void onEnable()
    {
        Bukkit.getServer().getPluginManager().registerEvents(this, this);
    }

    @EventHandler
    public void onVPNSuccession(VPNKickEvent event)
    {
        event.setMessage(ChatColor.RED + "VPNs are  not allowed on this server.");
        Bukkit.broadcast(ChatColor.translateAlternateColorCodes('&', "&6&lAntiVPN &8* &6" + event.getPlayer().getName() + " tried to join the server with a VPN."), "staff.permission");
    }
}