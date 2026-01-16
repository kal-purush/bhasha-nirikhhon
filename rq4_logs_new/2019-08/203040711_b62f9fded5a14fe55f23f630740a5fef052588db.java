package ca.fastis;

import org.bukkit.Server;
import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class CommandBlocker extends JavaPlugin implements Listener{
	Server server;
	
	public void onEnable() {
		server = this.getServer();
		server.getConsoleSender().sendMessage("Command Blocker Enabled");
	}
	
	public void onDisable() {
		server.getConsoleSender().sendMessage("Command Blocker Disabled");
	}
}