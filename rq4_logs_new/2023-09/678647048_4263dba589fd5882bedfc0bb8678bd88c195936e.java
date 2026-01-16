package net.okuri.qol.superItems.tools;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.okuri.qol.LoreGenerator;
import net.okuri.qol.PDCC;
import net.okuri.qol.PDCKey;
import net.okuri.qol.qolCraft.superCraft.SuperCraftable;
import net.okuri.qol.superItems.SuperItem;
import net.okuri.qol.superItems.SuperItemType;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

public class EnvGetter implements SuperCraftable {
    private final SuperItemType type = SuperItemType.ENV_TOOL;
    @Override
    public ItemStack getSuperItem() {
        ItemStack item = new ItemStack(Material.PAPER);
        ItemMeta meta = item.getItemMeta();
        meta.displayName(Component.text("Environment Getter").color(NamedTextColor.GREEN));
        LoreGenerator lore = new LoreGenerator();
        lore.addInfoLore("This tool can be used to get the environment of a block.");
        lore.addInfoLore("Right click a block to get the environment.");
        meta.lore(lore.generateLore());
        PDCC.set(meta, PDCKey.TYPE,this.type.toString());
        item.setItemMeta(meta);
        return item;
    }

    @Override
    public ItemStack getDebugItem(int... args) {
        return this.getSuperItem();
    }

    @Override
    public void setMatrix(ItemStack[] matrix) {
    }
}