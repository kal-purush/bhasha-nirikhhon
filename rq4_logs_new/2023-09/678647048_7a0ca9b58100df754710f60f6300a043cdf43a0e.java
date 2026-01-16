package net.okuri.qol.superItems;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.okuri.qol.LoreGenerator;
import net.okuri.qol.PDCKey;
import net.okuri.qol.PDCC;
import net.okuri.qol.qolCraft.superCraft.SuperCraftable;
import org.bukkit.Color;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.inventory.meta.PotionMeta;
import org.bukkit.potion.PotionEffect;
import org.bukkit.potion.PotionEffectType;

public class SakeIngredient implements SuperCraftable {
    private SuperItemType type = SuperItemType.SAKE_INGREDIENT;
    private ItemStack koji;
    private ItemStack rice;
    private ItemStack barley;
    private ItemStack potato;
    private double ricePolishingRatio;
    private final int maxDuration = 72000;
    private int registanceAmp;
    private int registanceDuration;
    private int fireResistAmp;
    private int fireResistDuration;
    private int regenAmp;
    private int regenDuration;
    private double x;
    private double y;
    private double z;
    private double smellRichness;
    private double tasteRichness;
    private double picularity;
    private double quality;
    private int rarity;
    private double temp;
    private double humid;
    private SuperItemType ingredientType;
    @Override
    public void setMatrix(ItemStack[] matrix) {
        this.koji = matrix[4];
        ItemStack ingredient = matrix[1];
        SuperItemType type = SuperItemType.valueOf(PDCC.get(ingredient.getItemMeta(), PDCKey.TYPE));
        if (type == SuperItemType.POLISHED_RICE){
            this.rice = ingredient;
        } else if (type == SuperItemType.BARLEY){
            this.barley = ingredient;
        } else if (type == SuperItemType.POTATO){
            this.potato = ingredient;
        }
        this.ingredientType = type;
        setting();
    }

    @Override
    public ItemStack getSuperItem() {
        ItemStack result = new ItemStack(Material.POTION);
        PotionMeta meta = (PotionMeta) result.getItemMeta();

        PDCC.setSuperItem(meta, this.type, this.x, this.y, this.z, this.quality, this.rarity, this.temp, this.humid);
        PDCC.set(meta, PDCKey.INGREDIENT_TYPE, this.ingredientType.toString());
        PDCC.set(meta, PDCKey.CONSUMABLE, false);

        meta.addCustomEffect(new PotionEffect(PotionEffectType.DAMAGE_RESISTANCE, this.registanceDuration, this.registanceAmp), true);
        meta.addCustomEffect(new PotionEffect(PotionEffectType.FIRE_RESISTANCE, this.fireResistDuration, this.fireResistAmp), true);
        meta.addCustomEffect(new PotionEffect(PotionEffectType.REGENERATION, this.regenDuration, this.regenAmp), true);

        meta.displayName(Component.text("Sake Ingredient").color(NamedTextColor.GOLD));
        meta.setColor(Color.WHITE);
        LoreGenerator lore = new LoreGenerator();
        lore.addInfoLore("Ingredient for Sake");
        lore.addParametersLore("X", this.x);
        lore.addParametersLore("Y", this.y);
        lore.addParametersLore("Z", this.z);
        lore.addParametersLore("Picularity", this.picularity);
        lore.addParametersLore("Quality", this.quality );
        lore.addRarityLore(this.rarity);
        if (ingredientType == SuperItemType.POLISHED_RICE){
            lore.addParametersLore("Rice Polishing Ratio", this.ricePolishingRatio, true);
        }
        meta.lore(lore.generateLore());
        meta.setCustomModelData(this.type.getCustomModelData());
        result.setItemMeta(meta);
        return result;
    }

    @Override
    public ItemStack getDebugItem(int... args) {
        this.koji = new Koji().getDebugItem(args);
        this.rice = new PolishedRice().getDebugItem(args);
        this.ingredientType = SuperItemType.POLISHED_RICE;
        setting();
        return this.getSuperItem();
    }

    private void setting(){
        ItemMeta meta;
        ItemMeta kojiMeta = koji.getItemMeta();
        if (ingredientType == SuperItemType.POLISHED_RICE){
            meta = rice.getItemMeta();
            double p = PDCC.get(kojiMeta, PDCKey.Z);
            this.picularity = -Math.pow(p, 2) + 2*p;
            this.ricePolishingRatio = ((double)PDCC.get(kojiMeta, PDCKey.RICE_POLISHING_RATIO) + (double)PDCC.get(meta, PDCKey.RICE_POLISHING_RATIO))/2.0;
        } else if(ingredientType == SuperItemType.BARLEY){
            meta = barley.getItemMeta();
            double p = PDCC.get(kojiMeta, PDCKey.Z);
            this.picularity = 4*(-Math.pow(p, 2) + p);
        } else if(ingredientType == SuperItemType.POTATO){
            meta = potato.getItemMeta();
            double p = PDCC.get(kojiMeta, PDCKey.Z);
            this.picularity = -Math.pow(p, 2) + 1;
        } else {
            return;
        }

        this.smellRichness = PDCC.get(kojiMeta, PDCKey.X);
        this.tasteRichness = PDCC.get(kojiMeta, PDCKey.Y);
        this.picularity = PDCC.get(kojiMeta, PDCKey.Z);
        this.quality = (double)PDCC.get(kojiMeta, PDCKey.QUALITY) * (double)PDCC.get(meta, PDCKey.QUALITY);

        this.temp = PDCC.get(meta, PDCKey.TEMP);
        this.humid = PDCC.get(meta, PDCKey.HUMID);

        this.x = (double)PDCC.get(meta, PDCKey.X) * this.picularity * this.quality;
        this.y = (double)PDCC.get(meta, PDCKey.Y) * this.picularity * this.quality;
        this.z = (double)PDCC.get(meta, PDCKey.Z) * this.picularity * this.quality;
        this.rarity = SuperItem.getRarity(this.x, this.y, this.z);

        this.registanceAmp = (int)(this.x * this.tasteRichness * 2.5);
        this.registanceDuration = (int)(this.x * this.smellRichness * this.maxDuration);
        this.fireResistAmp = (int)(this.y * this.tasteRichness * 2.5);
        this.fireResistDuration = (int)(this.y * this.smellRichness * this.maxDuration);
        this.regenAmp = (int)(this.z * this.tasteRichness * 2.5);
        this.regenDuration = (int)(this.z * this.smellRichness * this.maxDuration);
    }
}