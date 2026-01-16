package com.gmail.grzegorz2047.violationlogger;

import fr.neatmonster.nocheatplus.NCPEvent;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;

/**
 * Created by grzeg on 01.11.2016.
 */
public class NCPListener implements Listener {

    private final ViolationLogger plugin;

    public NCPListener(ViolationLogger plugin) {
        this.plugin = plugin;
    }

    @EventHandler
    public void onNCP(NCPEvent e) {
        for (Player p : Bukkit.getOnlinePlayers()) {
            if (p.hasPermission("cg.admin") || p.hasPermission("nocheatplus.admin")) {
                p.sendMessage(e.getMessage());
            }
        }
        //Configure prefix:violation:player:power: + arena
        String msg = e.getMessage().replaceAll("&[a-z]", "").replaceAll("§[a-z]", "");
        System.out.println(msg);
        String[] args = msg.split(":");
        if (args.length == 4) {
            if (args[3].length() <= 5) {
                int power = Integer.parseInt(args[3]);
                if (power >= 10) {
                    plugin.getSql().addViolation(args[1], args[2], power, plugin.getArenaName());
                }
            }
        }
    }
}