package me.Seisan.plugin.Features.commands.anothers;

import me.Seisan.plugin.Features.Chat.ChatFormat;
import me.Seisan.plugin.Main.Command;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.util.*;

public class PrefixCommand extends Command {

    private static Map<UUID, String> prefix = new HashMap<>();

    @Override
    public void myOnCommand(CommandSender sender, org.bukkit.command.Command command, String label, String[] args) {

        // /prefix <prefix>
        if ((sender instanceof Player)) {
            Player p = (Player) sender;

            // /prefix </> : Reset prefix
            if (args.length == 0) {
                prefix.remove(p.getUniqueId());
                p.sendMessage("§aPréfix reset.");
            }
            // /prefix <prefix> : Set prefix
            else if (args.length == 1) {
                if (ChatFormat.getPrefix().contains(args[0])) {
                    String prefixArgs = args[0];
                    prefix.put(p.getUniqueId(), prefixArgs);
                    p.sendMessage("§aPréfix défini sur " + prefixArgs);
                }
            } else
                p.sendMessage("§cUtilisation: /prefix <prefix> | Si vous voulez reset votre préfix, faites /prefix");

        }
    }

    public static String getPlayerDefaultPrefix(Player p) {
        return prefix.getOrDefault(p.getUniqueId(), "");
    }

    @Override
    protected List<String> myOnTabComplete(CommandSender sender, org.bukkit.command.Command command, String label, String[] split) {
        return new ArrayList<>();
    }

    @Override
    protected boolean isOpOnly() {
        return false;
    }
}