package systems.joey.manhunt.commands;


import net.md_5.bungee.api.chat.TextComponent;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import net.md_5.bungee.api.ChatMessageType;

public class TestCommand implements CommandExecutor {
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (sender instanceof Player player) {
            player.setHealth(0);
//            player.sendTitle("Test", "Test", 10, 10, 10);
//            player.spigot().sendMessage(ChatMessageType.ACTION_BAR, TextComponent.fromLegacyText("aaa"));
        }
        return true;
    }
}