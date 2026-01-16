package net.okuri.qol;

import org.bukkit.NamespacedKey;
import org.bukkit.block.TileState;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.persistence.PersistentDataType;

import java.util.Map;

public class PDCC {
    @Deprecated
    public static <T> T get(ItemStack item, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        if (!has(item, key)) throw new IllegalArgumentException("ItemStack does not have the key");
        return item.getItemMeta().getPersistentDataContainer().get(key.key, type);
    }
    public static <T> T get(ItemMeta meta, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        if (!has(meta, key)) throw new IllegalArgumentException("ItemMeta does not have the key");
        return meta.getPersistentDataContainer().get(key.key, type);
    }
    public static <T> T get(TileState tile, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.BLOCK) throw new IllegalArgumentException("PDCKey is not for TileState");
        if (!has(tile, key)) throw new IllegalArgumentException("TileState does not have the key");
        return tile.getPersistentDataContainer().get(key.key, type);
    }
    public static <T> T get(Player player, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.PLAYER) throw new IllegalArgumentException("PDCKey is not for Player");
        if (!has(player, key)) throw new IllegalArgumentException("Player does not have the key");
        return player.getPersistentDataContainer().get(key.key, type);
    }
    @Deprecated
    public static <T> boolean has(ItemStack item, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        return item.getItemMeta().getPersistentDataContainer().has(key.key, type);
    }
    public static <T> boolean has(ItemMeta meta, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        return meta.getPersistentDataContainer().has(key.key, type);
    }
    public static <T> boolean has(TileState tile, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.BLOCK) throw new IllegalArgumentException("PDCKey is not for TileState");
        return tile.getPersistentDataContainer().has(key.key, type);
    }
    public static <T> boolean has(Player player, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.PLAYER) throw new IllegalArgumentException("PDCKey is not for Player");
        return player.getPersistentDataContainer().has(key.key, type);
    }
    // 注意! ItemStackのやつを用いる時は、このあとにItemMetaを取得すること!予期せぬバグを防ぐため!
    // ItemMetaに統一することを推奨
    @Deprecated
    public static <T> void set(ItemStack item, PDCKey key, T value){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        ItemMeta meta = item.getItemMeta();
        meta.getPersistentDataContainer().set(key.key, type, value);
        item.setItemMeta(meta);
    }
    public static <T> void set(ItemMeta meta, PDCKey key, T value){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        meta.getPersistentDataContainer().set(key.key, type, value);
    }
    public static <T> void set(TileState tile, PDCKey key, T value){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.BLOCK) throw new IllegalArgumentException("PDCKey is not for TileState");
        tile.getPersistentDataContainer().set(key.key, type, value);
        tile.update();
    }
    public static <T> void set(Player player, PDCKey key, T value){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.PLAYER) throw new IllegalArgumentException("PDCKey is not for Player");
        player.getPersistentDataContainer().set(key.key, type, value);
    }
    @Deprecated
    public static <T> void remove(ItemStack item, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        ItemMeta meta = item.getItemMeta();
        meta.getPersistentDataContainer().remove(key.key);
        item.setItemMeta(meta);
    }
    public static <T> void remove(ItemMeta meta, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.ITEM) throw new IllegalArgumentException("PDCKey is not for ItemStack");
        meta.getPersistentDataContainer().remove(key.key);
    }
    public static <T> void remove(TileState tile, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.BLOCK) throw new IllegalArgumentException("PDCKey is not for TileState");
        tile.getPersistentDataContainer().remove(key.key);
        tile.update();
    }
    public static <T> void remove(Player player, PDCKey key){
        PersistentDataType<?,T> type = key.type;
        if (key.apply != PDCKey.ApplyType.PLAYER) throw new IllegalArgumentException("PDCKey is not for Player");
        player.getPersistentDataContainer().remove(key.key);
    }

}