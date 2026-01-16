package de.ender.meins_weapons;

import de.ender.core.ItemBuilder;
import de.ender.core.weapons.Weapon;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.Sound;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.block.data.type.Fire;
import org.bukkit.entity.Player;
import org.bukkit.event.entity.EntityDamageByEntityEvent;
import org.bukkit.event.entity.ProjectileHitEvent;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.ShapedRecipe;

import java.util.Random;

public class FlameThrower extends Weapon {


    public FlameThrower() {
        super("Flamethrower", Main.getPlugin(), 0.1f, 2, false, false,
                false, Sound.ITEM_FIRECHARGE_USE, 0.5f, 1.5f);
    }
    
    public ItemStack getItemStack() {
        return new ItemBuilder(Material.BLAZE_POWDER,1).setName("<aqua>"+getName()).build();
    }

    
    public ShapedRecipe getRecipe() {
        return super.getRecipe().shape(
                "IOT",
                " IB",
                "  B")
                .setIngredient('T',Material.GLASS_BOTTLE)
                .setIngredient('B',Material.IRON_BLOCK)
                .setIngredient('O',Material.OBSIDIAN)
                .setIngredient('I',Material.IRON_INGOT);
    }
    
    public void rangedEntityHit(Player player, EntityDamageByEntityEvent event) {
        event.getEntity().setFireTicks(new Random().nextInt(45)+30);
        super.rangedEntityHit(player, event);
    }
    
    public void useEffects(Player player, UseType useType) {
        shootItem(player,2,new ItemBuilder(Material.FIRE,1).build(),
                0.10F,0.2f,false,true,false);
    }

    @Override
    protected boolean hasAmmo(Player player) {
        return true;
    }
    @Override
    protected void removeAmmo(Player player) {

    }

    public ItemStack getAmmoItem() {
        return null;
    }
    
    public void rangedHit(Player player, ProjectileHitEvent event) {
        super.rangedHit(player, event);
        Block block = event.getHitBlock();
        BlockFace blockFace = event.getHitBlockFace();
        if(blockFace != null&& block!= null &&block.getType().isBurnable()) {
            Block fireblock = block.getRelative(blockFace);
            if(!fireblock.getType().toString().toLowerCase().contains("air")) return;
            fireblock.setType(Material.FIRE);
            Fire fire = (Fire) Bukkit.createBlockData(Material.FIRE);
            fire.getFaces().forEach((fireFace)->fire.setFace(fireFace, false));
            if(fire.getAllowedFaces().contains(blockFace.getOppositeFace())) fire.setFace(blockFace.getOppositeFace(),true);
            fireblock.setBlockData(fire);
        }
    }

    
    public boolean hasRequirements(Player player) {
        return true;
    }
}