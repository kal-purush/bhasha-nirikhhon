package net.runeduniverse.mc.plugins.traveler.commands;

import org.bukkit.ChatColor;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.annotation.command.Command;
import org.bukkit.plugin.java.annotation.command.Commands;

import lombok.RequiredArgsConstructor;
import net.runeduniverse.mc.plugins.traveler.data.model.Traveler;
import net.runeduniverse.mc.plugins.traveler.services.TravelerService;
import net.runeduniverse.mc.plugins.traveler.services.TravelerService.Info;

@Commands(@Command(name = "TravelerManagementCommand", aliases = "tnpc", usage = "/tnpc <Traveler ID> <options>"))
@RequiredArgsConstructor
public class TravelerCommandExecutor implements CommandExecutor {

	private final TravelerService service;

	@Override
	public boolean onCommand(CommandSender sender, org.bukkit.command.Command command, String label, String[] args) {
		if (label != "tnpc" || args.length < 1)
			return false;

		Long id = null;
		try {
			id = Long.parseLong(args[0]);
		} catch (NumberFormatException e) {
			sender.sendMessage(ChatColor.RED + "[ERROR] The value <" + args[0] + "> is not a valid number!");
			return true;
		}

		Traveler traveler = this.service.loadTraveler(id);
		if (traveler == null) {
			sender.sendMessage(ChatColor.RED + "[ERROR] Invalid Traveler ID <" + id
					+ "> received!\nThe Traveler ID can be optained through a journal or by searching the database provided by Snowflake!");
			return true;
		}
		Info info = this.service.getInfo(traveler);

		if (args.length < 2) {
			// tnpc <id> => shows info
			sender.sendMessage(info.full());
			return true;
		}

		switch (args[1]) {
		// tnpc <id> name => show name
		// tnpc <id> name set => open $Anvil-Rename-Inv
		// tnpc <id> name set <name> => rename with given name
		case "name":
			if (args.length < 3) {
				sender.sendMessage(info.name());
				return true;
			}
			switch (args[2]) {
			case "set":
				if (args.length < 4) {
					// TODO open $Anvil-Rename-Inv
					return true;
				}

				// TODO set name args[3]
				return true;
			}
			break;
		// tnpc <id> destname => show name
		// tnpc <id> destname set => open $Anvil-Rename-Inv
		// tnpc <id> destname set <name> => rename with given name
		case "destname":
			if (args.length < 3) {
				sender.sendMessage(info.destname());
				return true;
			}
			switch (args[2]) {
			case "set":
				if (args.length < 4) {
					// TODO open $Anvil-Rename-Inv
					return true;
				}

				// TODO set destination name args[3]
				return true;
			}
			break;
		// tnpc <id> visibility => show visibility
		// tnpc <id> visibility toggle => toggles visibility
		// tnpc <id> visibility set <public/private>
		case "visibility":
			if (args.length < 3) {
				sender.sendMessage(info.visibility());
				return true;
			}
			switch (args[2]) {
			case "toggle":
				// TODO toggle visibility
				return true;
			case "set":
				if (args.length < 4)
					return false;

				// TODO set visibility args[3]
				return true;
			}
			break;
		// tnpc <id> movement => show movement
		// tnpc <id> movement toggle => toggles movement
		// tnpc <id> movement set <yes/no>
		case "movement":
			if (args.length < 3) {
				sender.sendMessage(info.visibility());
				return true;
			}
			switch (args[2]) {
			case "toggle":
				// TODO toggle movement
				return true;
			case "set":
				if (args.length < 4)
					return false;

				// TODO set movement args[3]
				return true;
			}
			break;
		// tnpc <id> invulnerable => show invulnerable
		// tnpc <id> invulnerable toggle => toggles invulnerable
		// tnpc <id> invulnerable set <yes/no>
		case "invulnerable":
			if (args.length < 3) {
				sender.sendMessage(info.invulnerable());
				return true;
			}
			switch (args[2]) {
			case "toggle":
				// TODO toggle invulnerable
				return true;
			case "set":
				if (args.length < 4)
					return false;

				// TODO set invulnerable args[3]
				return true;
			}
			break;
		// tnpc <id> home => show home
		// tnpc <id> home set => open $Chest-Selection-Inv
		// tnpc <id> home set x y z => sets home to the given position (must be op)
		case "home":
			if (args.length < 3) {
				sender.sendMessage(info.home());
				return true;
			}
			switch (args[2]) {
			case "set":
				if (args.length < 6) {
					// TODO open $Chest-Selection-Inv
					return true;
				}

				// TODO set home args[3-5]
				return true;
			}
			break;
		// tnpc <id> dest => show destination
		// tnpc <id> dest set => sets destination to the player's current position
		// tnpc <id> dest set x y z => sets destination to the given position (must be
		// op)
		case "dest":
			if (args.length < 3) {
				sender.sendMessage(info.destination());
				return true;
			}
			switch (args[2]) {
			case "set":
				if (args.length < 6) {
					// TODO open $Chest-Selection-Inv
					return true;
				}

				// TODO set destination args[3-5]
				return true;
			}
			break;
		// tnpc <id> owner => show owner
		// tnpc <id> owner set <new owner> => set owner with given new owner
		// tnpc <id> owner add <new owner> => add given new owner
		// tnpc <id> owner del <new owner> => remove given new owner
		case "owner":
			if (args.length < 3) {
				sender.sendMessage(info.owner());
				return true;
			}
			switch (args[2]) {
			case "set":
				if (args.length < 4)
					return false;

				// TODO set owner args[3]
				return true;
			case "add":
				if (args.length < 4)
					return false;

				// TODO add owner args[3]
				return true;
			case "del":
				if (args.length < 4)
					return false;

				// TODO delete owner args[3]
				return true;
			}
			break;
		// tnpc <id> sysowner => set owner to system (must be op)
		case "sysowner":
			if (sender.isOp()) {
				// TODO set owner to ENV
				return true;
			}
		}
		return false;

	}

}