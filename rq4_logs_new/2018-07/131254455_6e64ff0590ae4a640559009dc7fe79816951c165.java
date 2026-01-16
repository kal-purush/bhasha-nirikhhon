package package;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.Listener;

import package.Main;

public class HelpOp implements CommandExecutor,Listener {

	private Main plugin = Main.getPlugin(Main.class);
	
    public HelpOp(Main plugin) { // when adding command just go to main class and type new HelpOp(this); and add the command in plugin.yml
        Bukkit.getPluginCommand("helpop").setExecutor(this);
        
        Bukkit.getPluginManager().registerEvents(this, plugin);
    }
	
    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String commandLabel, String[] args) {
    	String message = "";
		for (String part : args) {
		    if (message != "") message += " ";
		    message += part;
		}
		message = message.substring(0, message.length() - 0);
        message = message.replaceAll("&", "§");
		for (Player player : Bukkit.getServer().getOnlinePlayers()) {
			if (player.isOp()) {
				if(message == "") {
					sender.sendMessage(ChatColor.translateAlternateColorCodes('`', "`7`l«`4`lHelp`c`lOp`7`l» `7Please input a message to send the the op players!"));
					return true;
				}
				player.sendMessage(ChatColor.translateAlternateColorCodes('`', "`7`l«`4`lHelp`c`lOp`7`l» `r" + sender.getName() + "`7 »» " + message));
				sender.sendMessage(ChatColor.translateAlternateColorCodes('`', "`7`l«`4`lHelp`c`lOp`7`l» `r" + message));
				return true;
			} else {
				sender.sendMessage(ChatColor.translateAlternateColorCodes('`', "`7`l«`4`lHelp`c`lOp`7`l» `7Wasn't able to find any op players!"));
				return true;
			}
		}
		return true;
    	
    }
	
}