package net.teengamingnights.assemblymod;

import net.teengamingnights.assemblymod.factory.FactoryManager;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.block.Chest;
import org.bukkit.craftbukkit.v1_13_R2.block.CraftChest;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.inventory.ItemStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EventHandle implements Listener {
    private enum BlockType { FURNACE, CHEST }
    private enum FaceDirection { NS, EW }

    @EventHandler
    public void onPlayerClick(PlayerInteractEvent e) {
        Player p = e.getPlayer();
        Block b = e.getClickedBlock();
        BlockFace face = e.getBlockFace();
        if (b.getType() == Material.CRAFTING_TABLE) {
            Block furnace = null;
            Block chest = null;
            if (face == BlockFace.NORTH || face == BlockFace.SOUTH) {
                furnace = furnaceOrChest(FaceDirection.NS, BlockType.FURNACE, b);
                chest = furnaceOrChest(FaceDirection.NS, BlockType.CHEST, b);
            } else if (face == BlockFace.EAST || face == BlockFace.WEST) {
                furnace = furnaceOrChest(FaceDirection.EW, BlockType.FURNACE, b);
                chest = furnaceOrChest(FaceDirection.EW, BlockType.CHEST, b);
            } else
                return;
            if ((furnace != null && chest != null) && furnace != chest) {
                List<ItemStack> items = Arrays.asList(((Chest)(chest.getState())).getBlockInventory().getContents());
                AssemblyMod.getFactoryManager().createFactory(items, b);
            }
        }
    }

    private Block furnaceOrChest(FaceDirection direction, BlockType type, Block center) {
        World w = center.getWorld();
        int x = center.getX();
        int y = center.getY();
        int z = center.getZ();
        Block left;
        Block right;
        switch (direction) {
            case NS:
                left = w.getBlockAt(x + 1, y, z);
                right = w.getBlockAt(x - 1, y, z);
                break;
            case EW:
                left = w.getBlockAt(x, y, z + 1);
                right = w.getBlockAt(x, y, z - 1);
                break;
            default:
                return null;
        }
        switch (type) {
            case FURNACE:
                if (left.getType() == Material.FURNACE)
                    return left;
                if (right.getType() == Material.FURNACE)
                    return right;
                break;
            case CHEST:
                if (left.getType() == Material.CHEST)
                    return left;
                if (right.getType() == Material.CHEST)
                    return right;
        }
        return null;
    }

    private boolean hasValidMaterials(List<Block> blocks) {
        List<Material> materials = new ArrayList<>();
        for (Block b : blocks)
            materials.add(b.getType());
        return materials.contains(Material.FURNACE) && materials.contains(Material.CHEST);
    }
}