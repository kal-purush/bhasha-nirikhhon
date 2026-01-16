package nl.thedutchruben.mccore.spigot.ui;

import org.bukkit.event.inventory.InventoryType;

public class InventoryBuilder {
    private String title;
    private InventoryType inventoryType;
    public InventoryBuilder(String title) {
        this.title = title
    }

    public InventoryBuilder type(InventoryType type){
        inventoryType = type;
        return this;
    }

}