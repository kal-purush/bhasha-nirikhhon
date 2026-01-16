package com.djrapitops.permissionsex.backends;

import net.md_5.bungee.api.chat.ClickEvent;
import net.md_5.bungee.api.chat.ComponentBuilder;
import net.md_5.bungee.api.chat.HoverEvent;
import net.md_5.bungee.api.chat.TextComponent;
import org.bukkit.ChatColor;
import org.bukkit.command.CommandSender;
import ru.tehkode.permissions.bukkit.PermissionsEx;
import ru.tehkode.permissions.bukkit.commands.PermissionsCommand;
import ru.tehkode.permissions.commands.Command;

import java.util.Map;

public class DashboardCommand extends PermissionsCommand {

	@Command(name = "pex",
			syntax = "dashboard",
			permission = "permissions.dashboard",
			description = "Get link to the dashboard")
	public void dashboard(PermissionsEx plugin, CommandSender sender, Map<String, String> args) {
		boolean enabled = plugin.getDashboard().isEnabled();
		if (!enabled) {
			sendMessage(sender, ChatColor.RED + " Dashboard is not enabled. (Is the port in use?)");
			return;
		}
		String accessAddress = plugin.getDashboard().getWebServer().getAccessAddress();

		try {
			if (sender.spigot() == null) {
				sender.sendMessage("Dashboard Address: " + accessAddress);
			} else {
				TextComponent link = new TextComponent(accessAddress);
				link.setClickEvent(new ClickEvent(ClickEvent.Action.OPEN_URL, accessAddress));
				link.setHoverEvent(new HoverEvent(HoverEvent.Action.SHOW_TEXT, new ComponentBuilder(accessAddress).create()));
				sender.spigot().sendMessage(new ComponentBuilder("Dashboard Address: ").append(link).create());
			}
		} catch (NoClassDefFoundError | NoSuchMethodError e) {
			sender.sendMessage("Dashboard Address: " + accessAddress);
		}
	}

}