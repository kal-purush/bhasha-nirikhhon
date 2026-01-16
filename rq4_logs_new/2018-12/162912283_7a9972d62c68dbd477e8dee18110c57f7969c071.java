package net.teengamingnights.assemblymod;

import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerInteractEvent;

public class EventHandle implements Listener {
    @EventHandler
    public void onPlayerClick(PlayerInteractEvent e) {
    Player p = e.getPlayer();
    Block b = e.getClickedBlock();
    BlockFace face = e.getBlockFace();
    if (b.getType() == Material.CRAFTING_TABLE) {
        Material m1;
        Material m2;
        if (face == BlockFace.NORTH || face == BlockFace.SOUTH) {
            m1 = b.getWorld().getBlockAt(b.getX() + 1, b.getY(), b.getZ()).getType();
            m2 = b.getWorld().getBlockAt(b.getX() - 1, b.getY(), b.getZ()).getType();
        } else if (face == BlockFace.EAST || face == BlockFace.WEST) {
            m1 = b.getWorld().getBlockAt(b.getX(), b.getY(), b.getZ() + 1).getType();
            m2 = b.getWorld().getBlockAt(b.getX(), b.getY(), b.getZ() - 1).getType();
        } else
            return;
        p.sendMessage(m1.toString());
        p.sendMessage(m2.toString());
        if (isValidMaterial(m1) && isValidMaterial(m2))
            p.sendMessage("valid");
        else
            p.sendMessage("invalid");
        p.sendMessage("---");
    }
}

    private boolean isValidMaterial(Material mat) {
        return mat == Material.FURNACE || mat == Material.CHEST;
    }
}