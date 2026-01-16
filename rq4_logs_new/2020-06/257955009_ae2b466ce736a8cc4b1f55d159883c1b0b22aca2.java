package me.kr_gaming2005.staffmode.staffmodemain.Events;

import me.kr_gaming2005.staffmode.staffmodemain.ChatUtill;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryClickEvent;

public class InventoryClick implements Listener {
    @EventHandler
    public void onInvClick(InventoryClickEvent e){
        if(e.getView().getTitle().equalsIgnoreCase(ChatUtill.format("&e&lHidden Staff"))){
            e.setCancelled(true);
        }else if(e.getView().getTitle().equalsIgnoreCase(ChatUtill.format("&e&lOnline Players"))){
            e.setCancelled(true);
        }
    }


}