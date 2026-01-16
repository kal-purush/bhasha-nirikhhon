package br.com.minecart.listeners;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import br.com.minecart.Minecart;

public class Playerlistener implements Listener
{
    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event)
    {
        String username = event.getPlayer().getName().toLowerCase();
        Minecart.instance.cooldown.put(username, System.currentTimeMillis());
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event)
    {
        String username = event.getPlayer().getName().toLowerCase();
        Minecart.instance.cooldown.remove(username);
    }
}