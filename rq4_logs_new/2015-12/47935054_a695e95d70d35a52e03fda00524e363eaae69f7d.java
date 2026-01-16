package com.mistri.plugin;

import java.util.logging.Logger;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import net.md_5.bungee.api.ChatColor;

public class Punisher extends JavaPlugin {
	Logger log = Bukkit.getLogger();

	@Override
	public void onEnable() {

	}

	@Override
	public void onDisable() {

	}

	public boolean onCommand(CommandSender sender, Command cmd, String commandLabel, String[] args) {
		String prefix = (ChatColor.DARK_GRAY + "[" + ChatColor.DARK_RED + "Punisher" + ChatColor.DARK_GRAY + "] " + ChatColor.GRAY);
		String error = (ChatColor.DARK_RED + "");

		if (sender instanceof Player) {
			Player p = (Player) sender;
			if (commandLabel.equalsIgnoreCase("punisher")) {
				p.sendMessage(prefix + "Punisher v1.0 made by " + ChatColor.GOLD + "Mistri");
				p.sendMessage(ChatColor.RED + "Usage:");
				p.sendMessage(ChatColor.RED + "/punisher " + ChatColor.GRAY + "- " + ChatColor.GOLD + "Bring up this help menu.");
				p.sendMessage(ChatColor.RED + "/hurt <player> <amount> " + ChatColor.GRAY + "- " + ChatColor.GOLD + "Hurts a player by the amount of hearts given. Supports decimals.");
				p.sendMessage(ChatColor.RED + "/starve <player> <amount> " + ChatColor.GRAY + "- " + ChatColor.GOLD + "Starves a player by the amount of food bars given. Supports decimals.");
				p.sendMessage(ChatColor.RED + "/punish <player> <amount> " + ChatColor.GRAY + "- " + ChatColor.GOLD + "Hurts and starves a player by the same amount.");
			}


			if (p.hasPermission("punisher.hurt")) {
				if (commandLabel.equalsIgnoreCase("hurt")) {
					if (args.length == 2) {
						if (Bukkit.getPlayer(args[0]) != null) {
							Player target = Bukkit.getPlayer(args[0]);
							try {
								if ((Double.parseDouble(args[1]) * 2) >= 0 && Math.abs((Double.parseDouble(args[1]) * 2) - target.getMaxHealth()) <= target.getMaxHealth()) {
									target.damage((Double.parseDouble(args[1]) * 2));
									p.sendMessage(prefix + "Successfully damaged " + args[0] + " by " + args[1]);
								}
								else p.sendMessage(prefix + error + "Hearts must be between 0 and " + (p.getMaxHealth() / 2));
							}
							catch(NumberFormatException exception) {
								p.sendMessage(prefix + error + args[1] + " is not a valid number.");
							}
						}
						else p.sendMessage(prefix + error + args[0] + " is not online.");
					}
					else p.sendMessage(prefix + error + "Improper usage of command. Do " + ChatColor.GOLD + " /punisher" + error + "to view the correct usage.");
				}
			}

			if (p.hasPermission("punisher.starve")) {
				if (commandLabel.equalsIgnoreCase("starve")) {
					if (args.length == 2) {
						if (Bukkit.getPlayer(args[0]) != null) {
							try {
								Player target = Bukkit.getPlayer(args[0]);
								target.setFoodLevel((int) (target.getFoodLevel() - (Double.parseDouble(args[1]) * 2)));
								p.sendMessage(prefix + "Successfully starved " + args[0] + " by " + args[1]);
							}
							catch(NumberFormatException exception) {
								p.sendMessage(prefix + error + args[1] + " is not a valid number.");
							}
						}
						else p.sendMessage(prefix + error + args[0] + " is not online.");
					}
					else p.sendMessage(prefix + error + "Improper usage of command. Do " + ChatColor.GOLD + " /punisher" + error + "to view the correct usage.");
				}
			}

			if (p.hasPermission("punisher.punish")) {
				if (commandLabel.equalsIgnoreCase("punish")) {
					if (args.length == 2) {
						if (Bukkit.getPlayer(args[0]) != null) {
							Player target = Bukkit.getPlayer(args[0]);
							try {
								if ((Double.parseDouble(args[1]) * 2) >= 0 && Math.abs((Double.parseDouble(args[1]) * 2) - target.getMaxHealth()) <= target.getMaxHealth()) {
									target.damage((Double.parseDouble(args[1]) * 2));
									target.setFoodLevel((int) (target.getFoodLevel() - (Double.parseDouble(args[1]) * 2)));
									p.sendMessage(prefix + "Successfully damaged and starved " + args[0] + " by " + args[1]);
								}
								else p.sendMessage(prefix + error + "Hearts must be between 0 and " + (p.getMaxHealth() / 2));
							}
							catch(NumberFormatException exception) {
								p.sendMessage(prefix + error + args[1] + " is not a valid number.");
							}
						}
						else p.sendMessage(prefix + error + args[0] + " is not online.");
					}
					else p.sendMessage(prefix + error + "Improper usage of command. Do " + ChatColor.GOLD + " /punisher" + error + "to view the correct usage.");
				}
			}
		}




		return true;
	}

}