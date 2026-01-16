package net.okuri.qol.drinks;

import net.okuri.qol.LoreGenerator;
import net.okuri.qol.superCraft.SuperCraftable;
import org.bukkit.NamespacedKey;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.PotionMeta;
import org.bukkit.persistence.PersistentDataType;

public class StrongZero extends SuperCraftable {
    private ItemStack[] matrix = null;
    private ItemStack rice = null;
    private ItemStack soda = null;
    public static NamespacedKey st0key = new NamespacedKey("qol", "strongzero");

    @Override
    public void setMatrix(ItemStack[] matrix){
        this.matrix = matrix;
        setting(matrix[4], matrix[7]);
    }

    private void setting(ItemStack rice, ItemStack soda){
        this.rice = rice;
        this.soda = soda;
    }

    @Override
    public ItemStack getSuperItem() {
        ItemStack strongZero = new ItemStack(this.rice.getType(), 1);
        PotionMeta meta = (PotionMeta)strongZero.getItemMeta();
        meta.getPersistentDataContainer().set(st0key, PersistentDataType.STRING, "strongzero");
        meta.setDisplayName("Strong Zero");
        LoreGenerator lore = new LoreGenerator();
        lore.addInfoLore("This is japanese culture!!!!!");
        lore.addImportantLore("SO STRONG");
        meta.lore(lore.generateLore());
        strongZero.setItemMeta(meta);

        return strongZero;
    }
}