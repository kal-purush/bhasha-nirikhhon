package de.iani.cubequest.commands;

import de.iani.cubequest.CubeQuest;
import de.iani.cubequest.interaction.Interactor;
import de.iani.cubequest.interaction.PlayerInteractInteractorEvent;
import de.iani.cubequest.util.ChatAndTextUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.player.PlayerInteractEvent;

public class SetOrRemoveInteractorAliasCommand extends SubCommand implements Listener {
    
    public static final String SET_COMMAND_PATH = "setInteractorAlias";
    public static final String FULL_SET_COMMAND = "quest " + SET_COMMAND_PATH;
    
    public static final String REMOVE_COMMAND_PATH = "removeInteractorAlias";
    public static final String FULL_REMOVE_COMMAND = "quest " + REMOVE_COMMAND_PATH;
    
    private boolean set;
    private Map<UUID, Interactor> firstSelections;
    
    public SetOrRemoveInteractorAliasCommand(boolean set) {
        this.set = set;
        this.firstSelections = new HashMap<>();
        
        Bukkit.getPluginManager().registerEvents(this, CubeQuest.getInstance());
        CubeQuest.getInstance().getEventListener().addOnPlayerQuit(player -> this.firstSelections.remove(player.getUniqueId()));
    }
    
    @Override
    public boolean onCommand(CommandSender sender, Command command, String alias, String commandString, ArgsParser args) {
        Player player = (Player) sender;
        if (this.firstSelections.containsKey(player.getUniqueId())) {
            ChatAndTextUtil.sendWarningMessage(sender, "Du " + (this.set ? "setzt" : "entfernst") + " bereits einen Alias.");
        }
        
        this.firstSelections.put(player.getUniqueId(), null);
        ChatAndTextUtil.sendNormalMessage(sender, "Bitte wähle den Alias durch Rechtsklick aus. Klicke links zum Abbrechen.");
        return true;
    }
    
    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, ArgsParser args) {
        return Collections.emptyList();
    }
    
    @EventHandler(priority = EventPriority.LOW, ignoreCancelled = true)
    public void onPlayerInteractInteractorEvent(PlayerInteractInteractorEvent<?> event) {
        if (!this.firstSelections.containsKey(event.getPlayer().getUniqueId())) {
            return;
        }
        
        event.setCancelled(true);
        if (this.set) {
            Interactor alias = this.firstSelections.remove(event.getPlayer().getUniqueId());
            if (alias == null) {
                this.firstSelections.put(event.getPlayer().getUniqueId(), event.getOriginalInteractor());
                ChatAndTextUtil.sendNormalMessage(event.getPlayer(), "Bitte wähle das Original durch Rechtsklick aus. Klicke links zum Abbrechen.");
                return;
            }
            boolean replaced = CubeQuest.getInstance().setAlias(alias, event.getOriginalInteractor()) != null;
            ChatAndTextUtil.sendNormalMessage(event.getPlayer(), "Alias " + (replaced ? "ersetzt" : "gesetzt."));
            return;
        } else {
            this.firstSelections.remove(event.getPlayer().getUniqueId());
            if (CubeQuest.getInstance().removeAlias(event.getOriginalInteractor())) {
                ChatAndTextUtil.sendNormalMessage(event.getPlayer(), "Alias entfernt.");
            } else {
                ChatAndTextUtil.sendWarningMessage(event.getPlayer(), "Dieser Interactor hatte keinen Alias.");
            }
            return;
        }
    }
    
    @EventHandler(priority = EventPriority.LOWEST)
    public void onPlayerInteractEvent(PlayerInteractEvent event) {
        if (event.getAction() == Action.LEFT_CLICK_AIR || event.getAction() == Action.LEFT_CLICK_BLOCK) {
            if (this.firstSelections.containsKey(event.getPlayer().getUniqueId())) {
                this.firstSelections.remove(event.getPlayer().getUniqueId());
                event.setCancelled(true);
                ChatAndTextUtil.sendNormalMessage(event.getPlayer(), "Alais-" + (this.set ? "Setzen" : "Entfernen") + " abgebrochen.");
            }
        }
    }
    
    @Override
    public String getRequiredPermission() {
        return CubeQuest.EDIT_QUESTS_PERMISSION;
    }
    
    @Override
    public boolean requiresPlayer() {
        return true;
    }
    
}