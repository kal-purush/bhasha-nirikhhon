package net.okuri.qol;

import org.bukkit.NamespacedKey;
import org.bukkit.block.BlockState;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.persistence.PersistentDataHolder;
import org.bukkit.persistence.PersistentDataType;


public enum PDCKey {
    // ここにPDCのキーを列挙する

    TYPE("super_item_type", PersistentDataType.STRING, ApplyType.ALL),
    // TYPE: SuperItemTypeを記憶する
    PROTECTED("qol_protected", PersistentDataType.BOOLEAN, ApplyType.BLOCK),
    // PROTECTED: 保護されているかどうかを記憶する
    ALCOHOL_LEVEL("alcohol_level", PersistentDataType.DOUBLE, ApplyType.PLAYER),
    // ALCOHOL_LEVEL: プレイヤーの血中アルコール濃度を記憶する
    ALCOHOL_AMOUNT("alcohol_amount", PersistentDataType.DOUBLE, ApplyType.ITEM),
    // ALCOHOL_AMOUNT: アルコール飲料の量を記憶する
    ALCOHOL_PERCENT("alcohol_percent", PersistentDataType.DOUBLE, ApplyType.ITEM),
    // ALCOHOL_PERCENT: アルコール飲料のアルコール度数を記憶する
    ALCOHOL("alcohol", PersistentDataType.BOOLEAN, ApplyType.ITEM),
    // ALCOHOL: アルコール飲料かどうかを記憶する
    FOOD_LEVEL("food_level", PersistentDataType.INTEGER, ApplyType.ITEM),
    // FOOD_LEVEL: 食料の満腹度回復量を記憶する
    FOOD_SATURATION("food_saturation", PersistentDataType.FLOAT, ApplyType.ITEM),
    // FOOD_SATURATION: 食料の隠し満腹度回復量を記憶する
    FOOD_SOUND("food_sound", PersistentDataType.STRING, ApplyType.ITEM),
    // FOOD_SOUND: 食料の食べたときの音を記憶する
    EATABLE("eatable", PersistentDataType.BOOLEAN, ApplyType.ITEM),
    // EATABLE: 食べられるかどうかを記憶する。ふつう食糧と扱われないアイテム用。
    CONSUMABLE("consumable", PersistentDataType.BOOLEAN, ApplyType.ITEM),
    // CONSUMABLE: 飲食可能かどうかを記憶する。ふつう食糧と扱われるアイテム用。falseの場合は飲食できない。
    X("x", PersistentDataType.DOUBLE, ApplyType.ALL),
    // X: パラメータ計算用(1)
    Y("y", PersistentDataType.DOUBLE, ApplyType.ALL),
    // Y: パラメータ計算用(2)
    Z("z", PersistentDataType.DOUBLE, ApplyType.ALL),
    // Z: パラメータ計算用(3)
    DIVLINE("divline", PersistentDataType.DOUBLE, ApplyType.ALL),
    // DIVLINE: パラメータ計算用(4)
    SODA_STRENGTH("soda_strength", PersistentDataType.DOUBLE, ApplyType.ITEM),
    // SODA_STRENGTH: ソーダの強さを記憶する
    NAME("name", PersistentDataType.STRING, ApplyType.ALL),
    // NAME: 生産者の名前を記憶する
    TEMP("temp", PersistentDataType.DOUBLE, ApplyType.ALL),
    // TEMP: 生産地の気温を記憶する
    HUMID("humid", PersistentDataType.DOUBLE, ApplyType.ALL),
    // HUMID: 生産地の湿度を記憶する
    BIOME_ID("biome_id", PersistentDataType.INTEGER, ApplyType.ALL),
    // BIOME_ID: 生産地のバイオームIDを記憶する
    QUALITY("quality", PersistentDataType.DOUBLE, ApplyType.ALL),
    // QUALITY: 品質。生産者の技量によって決まる
    RARITY("rarity", PersistentDataType.DOUBLE, ApplyType.ALL),
    // RARITY: 希少性。乱数によって決まる
    DISTILLATION("distillation", PersistentDataType.INTEGER, ApplyType.ITEM),
    // DISTILLATION: 蒸留の回数。蒸留酒の場合のみ記憶する
    MATURATION("maturation", PersistentDataType.INTEGER, ApplyType.ITEM);
    // MATURATION: 熟成の日数。熟成酒の場合のみ記憶する

    private final NamespacedKey key;
    private final PersistentDataType type;
    private final ApplyType apply;

    PDCKey(String key, PersistentDataType type, ApplyType apply) {
        this.key = new NamespacedKey("qol", key);
        this.type = type;
        this.apply = apply;
    }

    public NamespacedKey getKey() {
        return key;
    }
    public void setPDC(ItemStack item, Object value) {
        if (apply == ApplyType.ALL || apply == ApplyType.ITEM) {
            if (isSettable(value)) {
                ItemMeta meta = item.getItemMeta();
                PersistentDataContainer pdc = meta.getPersistentDataContainer();
                pdc.set(key, type, value);
                item.setItemMeta(meta);
            }
        }
    }
    public void setPDC(BlockState state, Object value){
        if (state instanceof PersistentDataHolder){
            if (apply == ApplyType.ALL || apply == ApplyType.BLOCK) {
                if (isSettable(value)) {
                    PersistentDataContainer pdc = ((PersistentDataHolder) state).getPersistentDataContainer();
                    pdc.set(key, type, value);
                    state.update();
                }
            }
        }
    }
    public void setPDC(Player player, Object value){
        if (apply == ApplyType.ALL || apply == ApplyType.PLAYER) {
            if (isSettable(value)) {
                PersistentDataContainer pdc = player.getPersistentDataContainer();
                pdc.set(key, type, value);
            }
        }
    }

    public Object getPDC(ItemStack item){
        if (apply == ApplyType.ALL || apply == ApplyType.ITEM) {
            ItemMeta meta = item.getItemMeta();
            PersistentDataContainer pdc = meta.getPersistentDataContainer();
            if (pdc.has(key, type)) {
                return pdc.get(key, type);
            }
        }
        return null;
    }
    public Object getPDC(BlockState state){
        if (state instanceof PersistentDataHolder){
            if (apply == ApplyType.ALL || apply == ApplyType.BLOCK) {
                PersistentDataContainer pdc = ((PersistentDataHolder) state).getPersistentDataContainer();
                if (pdc.has(key, type)) {
                    return pdc.get(key, type);
                }
            }
        }
        return null;
    }
    public Object getPDC(Player player){
        if (apply == ApplyType.ALL || apply == ApplyType.PLAYER) {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc.has(key, type)) {
                return pdc.get(key, type);
            }
        }
        return null;
    }

    public boolean hasPDC(ItemStack item){
        if (apply == ApplyType.ALL || apply == ApplyType.ITEM) {
            ItemMeta meta = item.getItemMeta();
            PersistentDataContainer pdc = meta.getPersistentDataContainer();
            return pdc.has(key, type);
        }
        return false;
    }
    public boolean hasPDC(BlockState state){
        if (state instanceof PersistentDataHolder){
            if (apply == ApplyType.ALL || apply == ApplyType.BLOCK) {
                PersistentDataContainer pdc = ((PersistentDataHolder) state).getPersistentDataContainer();
                return pdc.has(key, type);
            }
        }
        return false;
    }
    public boolean hasPDC(Player player){
        if (apply == ApplyType.ALL || apply == ApplyType.PLAYER) {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            return pdc.has(key, type);
        }
        return false;
    }

    private boolean isSettable(Object value){
        return value.getClass() == type.getPrimitiveType();
    }

    private enum ApplyType{
        ALL,
        ITEM,
        BLOCK,
        PLAYER;
    }
}
