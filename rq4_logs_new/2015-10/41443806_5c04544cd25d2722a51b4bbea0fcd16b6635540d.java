/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.bl4ckskull666.customs.commands;

import de.bl4ckskull666.customs.Customs;
import de.bl4ckskull666.customs.utils.Rnd;
import de.bl4ckskull666.mu1ti1ingu41.Language;
import java.util.UUID;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

/**
 *
 * @author PapaHarni
 */
public class Tppos implements CommandExecutor {
    @Override
    public boolean onCommand(CommandSender s, Command c, String l, String[] a) {
        UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000000");
        if(s instanceof Player)
            uuid = ((Player)s).getUniqueId();
        
        Player p = null;
        World w = null;
        Integer x = null;
        Integer y = null;
        Integer z = null;
        
        for(String arg: a) {
            if(Bukkit.getPlayer(arg) != null)
                p = Bukkit.getPlayer(arg);
            else if(Bukkit.getWorld(arg) != null)
                w = Bukkit.getWorld(arg);
            else if(Rnd.isNumeric(arg)) {
                if(x == null)
                    x = Integer.parseInt(arg);
                else if(y == null)
                    y = Integer.parseInt(arg);
                else if(z == null)
                    z = Integer.parseInt(arg);
            }
        }
        
        if(p == null) {
            if(s instanceof Player)
                p = (Player)s;
            else {
                //No player found
                s.sendMessage(Language.getMessage(Customs.getPlugin(), uuid, "command.tppos.console-sender", "This command need a online player if you send it from console."));
                return true;
            }
        }
        
        if(w == null)
            w = p.getWorld();
        
        if(x == null || y == null) {
            //Need position
            s.sendMessage(Language.getMessage(Customs.getPlugin(), uuid, "command.tppos.need-min-pos", "Need minimum a X and Z position. Optional: Player World X Y Z"));
            return true;
        }
        
        if(z == null) {
            z = y;
            y = null;
        }
        
        Location loc = new Location(w, x, ((y == null)?1:y), z);
        boolean isBeforeAir = loc.getBlock().getType().equals(Material.AIR);
        for(int i = loc.getBlockY(); i < 255; i++) {
            if(loc.getBlock().getRelative(0, i, 0).getType().equals(Material.AIR)) {
                if(isBeforeAir) {
                    loc = loc.getBlock().getRelative(0, i-1, 0).getLocation();
                    break;
                }
                isBeforeAir = true;
            } else
                isBeforeAir = false;
        }
        
        if(s.getName().equalsIgnoreCase(p.getName()))
            p.sendMessage(Language.getMessage(Customs.getPlugin(), p.getUniqueId(), "command.tppos.self", "You've been teleported to %world% %x%:%y%:%z%.", new String[] {"%world%","%x%","%y%","%z%"}, new String[] {loc.getWorld().getName(), String.valueOf(loc.getBlockX()), String.valueOf(loc.getBlockY()), String.valueOf(loc.getBlockZ())}));
        else {
            p.sendMessage(Language.getMessage(Customs.getPlugin(), p.getUniqueId(), "command.tppos.by-other", "You've been teleported by %name% to %world% %x%:%y%:%z%.", new String[] {"%world%","%x%","%y%","%z%","%name%"}, new String[] {loc.getWorld().getName(), String.valueOf(loc.getBlockX()), String.valueOf(loc.getBlockY()), String.valueOf(loc.getBlockZ()), s.getName()}));
            s.sendMessage(Language.getMessage(Customs.getPlugin(), uuid, "command.tppos.other", "You've been teleported %name% to %world% %x%:%y%:%z%.", new String[] {"%world%","%x%","%y%","%z%","%name%"}, new String[] {loc.getWorld().getName(), String.valueOf(loc.getBlockX()), String.valueOf(loc.getBlockY()), String.valueOf(loc.getBlockZ()), p.getName()}));
        }
        p.teleport(loc);
        return true;
    }
}