package org.sean.hologram;

import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

public class ShowSupportedColors implements CommandExecutor {

    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {

        Player plr = (Player) sender;

        // holocolor

        if (plr.hasPermission("hologram.use")) {

            if (cmd.getName().equals("holocolors")) {
                plr.sendMessage(ChatColor.GREEN + "==============================\n" +
                        ChatColor.WHITE + "&0 = " + ChatColor.BLACK + "Black" + "          " + ChatColor.WHITE + "&1 = " + ChatColor.DARK_BLUE + "Dark Blue\n" +
                        ChatColor.WHITE + "&2 = " + ChatColor.DARK_GREEN + "Dark Green" + "  " + ChatColor.WHITE + "&3 = " + ChatColor.DARK_AQUA + "Dark Aqua\n" +
                        ChatColor.WHITE + "&4 = " + ChatColor.DARK_RED + "Dark Red" + "     " + ChatColor.WHITE + "&5 = " + ChatColor.DARK_PURPLE + "Dark Purple\n" +
                        ChatColor.WHITE + "&6 = " + ChatColor.GOLD + "Gold" + "           " + ChatColor.WHITE + "&7 = " + ChatColor.GRAY + "Gray\n" +
                        ChatColor.WHITE + "&8 = " + ChatColor.DARK_GRAY + "Dark Gray" + "   " + ChatColor.WHITE + "&9 = " + ChatColor.BLUE + "Blue\n" +
                        ChatColor.WHITE + "&a = " + ChatColor.GREEN + "Green" + "        " + ChatColor.WHITE + "&b = " + ChatColor.AQUA + "Aqua\n" +
                        ChatColor.WHITE + "&c = " + ChatColor.RED + "Red" + "           " + ChatColor.WHITE + "&d = " + ChatColor.LIGHT_PURPLE + "Light Purple\n" +
                        ChatColor.WHITE + "&e = " + ChatColor.YELLOW + "Yellow" + "        " + ChatColor.WHITE + "&f = " + ChatColor.WHITE + "White\n" +
                        ChatColor.GREEN + "==============================\n");

            }
        }

        return false;
    }

}