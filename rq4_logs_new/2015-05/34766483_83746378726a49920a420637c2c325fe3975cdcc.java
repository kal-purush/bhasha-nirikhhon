package net.sparkzz.servercontrol.util;

import org.spongepowered.api.text.Texts;
import org.spongepowered.api.text.format.TextColors;
import org.spongepowered.api.util.command.CommandSource;

import java.util.UUID;

/**
 * @author Brendon
 * @since April 30, 2015
 */
public enum Messenger {
	COMMAND, ACTION;

	@Deprecated
	public String legacyFormatter(String message) {
		return message.replaceAll("&([0-9a-r])", "\u00A7$1");
	}

	public void deny(CommandSource source, Messenger type) {
		switch (type) {
			case COMMAND:
				source.sendMessage(Texts.of(TextColors.RED + "You are not permitted to use this command!"));
				break;
			case ACTION:
				source.sendMessage(Texts.of(TextColors.RED + "You are not permitted to perform that action!"));
				break;
		}
	}

	public void deny(CommandSource source, String suffix) {
		source.sendMessage(Texts.of(TextColors.RED + "You are not permitted to " + suffix));
	}

	public void notFound(CommandSource source, String name) {
		source.sendMessage(Texts.of(TextColors.RED + "Player " + TextColors.GOLD + name + TextColors.RED + " could not be found!"));
	}

	public void notFound(CommandSource source, UUID uuid) {
		source.sendMessage(Texts.of(TextColors.RED + "Player with the UUID of " + TextColors.GOLD + uuid + TextColors.RED + " could not be found!"));
	}

	public void warn(CommandSource source, String message) {
		source.sendMessage(Texts.of(TextColors.RED + message));
	}
}