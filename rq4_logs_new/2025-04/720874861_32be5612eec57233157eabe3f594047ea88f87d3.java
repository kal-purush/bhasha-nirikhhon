//Created by Maurice H. at 27.04.2025
package eu.lotusgc.mc.command;

import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import eu.lotusgc.mc.main.LotusController;
import eu.lotusgc.mc.main.Main;
import eu.lotusgc.mc.misc.Prefix;

public class SpeedCommand implements CommandExecutor{

	@Override
	public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
		if(!(sender instanceof Player)) {
			sender.sendMessage(Main.consoleSend);
		}else {
			Player player = (Player)sender;
			LotusController lc = new LotusController();
			if(args.length == 1) {
				if(player.hasPermission("lgc.speed")) {
					String speedVal = args[0];
					if(speedVal.equalsIgnoreCase("reset")) {
	                    //player.sendMessage(lc.getPrefix(Prefix.MAIN) + "§aYour speed has been reset to default.");
	                    if(player.isFlying()) {
	                    	player.setFlySpeed(0.2f);
	                    	lc.sendMessageReady(player, "command.speed.resetfly");
	                    }else {
	                    	player.setWalkSpeed(0.2f);
	                    	lc.sendMessageReady(player, "command.speed.resetwalk");
	                    }
					}else if(speedVal.matches("^[0-9]+$")) {
						int speed = Integer.parseInt(speedVal);
						if(player.isFlying()) {
							switch (speed) {
							case 1: player.setFlySpeed(0.1f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 2: player.setFlySpeed(0.2f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 3: player.setFlySpeed(0.3f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 4: player.setFlySpeed(0.4f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 5: player.setFlySpeed(0.5f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 6: player.setFlySpeed(0.6f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 7: player.setFlySpeed(0.7f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 8: player.setFlySpeed(0.8f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 9: player.setFlySpeed(0.9f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							case 10: player.setFlySpeed(1.0f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.fly").replace("%speed%", "" + speed)); break;
							default: lc.sendMessageReady(player, "command.speed.invalidInput"); break;
							}
						}else {
							switch (speed) {
							case 1: player.setWalkSpeed(0.1f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 2: player.setWalkSpeed(0.2f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 3: player.setWalkSpeed(0.3f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 4: player.setWalkSpeed(0.4f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 5: player.setWalkSpeed(0.5f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 6: player.setWalkSpeed(0.6f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 7: player.setWalkSpeed(0.7f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 8: player.setWalkSpeed(0.8f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 9: player.setWalkSpeed(0.9f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							case 10: player.setWalkSpeed(1.0f); player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "command.speed.walk").replace("%speed%", "" + speed)); break;
							default: lc.sendMessageReady(player, "command.speed.invalidInput"); break;
							}
						}
					}else {
						lc.noPerm(player, "lgc.speed");
					}
				}else {
					player.sendMessage(lc.getPrefix(Prefix.MAIN) + lc.sendMessageToFormat(player, "global.args") + "§7/speed <1 - 9 | reset>");
				}
			}
		}
		return true;
	}
}