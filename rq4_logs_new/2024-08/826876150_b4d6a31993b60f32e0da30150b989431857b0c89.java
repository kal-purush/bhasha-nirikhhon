package me.Seisan.plugin.Features.commands.anothers;

import me.Seisan.plugin.Features.commands.anothers.Commands;
import me.Seisan.plugin.Main.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.util.ArrayList;
import java.util.List;

public class TechniqueMJCommand extends Command {
    @Override
    public void myOnCommand(CommandSender sender, org.bukkit.command.Command command, String label, String[] split) {
        if (sender instanceof Player) {
            Player player = (Player) sender;
            //message d'erreur si il y a pas d'argument dans la commande !
            if (split.length == 0) {
                player.sendMessage("§4HRP : §7Il faut faire l0a commande /techniquemj <nom de la technique>");
            }
            //message si il y a argument / nom de la technique !
            if (split.length >= 1) {
                StringBuilder tc = new StringBuilder();
                for (String part : split) {
                    tc.append(part + " ");
                }
                player.sendMessage("§c** " + player.getDisplayName() + " §r§créalise la technique " + "§d§l" + tc.toString());
                int range = 30;

            }

        }


    }

    @Override
    protected List<String> myOnTabComplete(CommandSender sender, org.bukkit.command.Command command, String
            label, String[] split) {
        return List.of();
    }


}



