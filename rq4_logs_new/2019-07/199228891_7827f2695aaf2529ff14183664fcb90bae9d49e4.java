package com.eonzenx.modsetup.armor;

import com.eonzenx.tats.TATsMod;

import net.minecraft.inventory.EquipmentSlotType;
import net.minecraft.item.IArmorMaterial;
import net.minecraft.item.Item;
import net.minecraft.item.Items;
import net.minecraft.item.crafting.Ingredient;
import net.minecraft.util.SoundEvent;

public enum ArmorMaterialList implements IArmorMaterial {
	
	// Ores armor
	EMERALD("emerald", 22, new int[]{2, 5, 7, 2}, 21, "item.armor.equip_iron", 1F, Items.EMERALD),
	REDSTONE("redstone", 8, new int[]{1, 3, 5, 2}, 6, "item.armor.equip_chain", 0F, Items.REDSTONE_BLOCK),
	LAPIS_LAZULI("lapis_lazuli", 18, new int[]{2, 5, 7, 2}, 35, "item.armor.equip_gold", 1.5F, Items.LAPIS_LAZULI);
	/*
	LEATHER("leather", 5, new int[]{1, 2, 3, 1}, 15, SoundEvents.ITEM_ARMOR_EQUIP_LEATHER, 0.0F
	CHAIN("chainmail", 15, new int[]{1, 4, 5, 2}, 12, SoundEvents.ITEM_ARMOR_EQUIP_CHAIN, 0.0F
	IRON("iron", 15, new int[]{2, 5, 6, 2}, 9, SoundEvents.ITEM_ARMOR_EQUIP_IRON, 0.0F
	GOLD("gold", 7, new int[]{1, 3, 5, 2}, 25, SoundEvents.ITEM_ARMOR_EQUIP_GOLD, 0.0F
	DIAMOND("diamond", 33, new int[]{3, 6, 8, 3}, 10, SoundEvents.ITEM_ARMOR_EQUIP_DIAMOND, 2.0F
	TURTLE("turtle", 25, new int[]{2, 5, 6, 2}, 9, SoundEvents.ITEM_ARMOR_EQUIP_TURTLE, 0.0F
	 */
	
	private static final int[] MAX_DAMAGE_ARRAY = new int[] {13, 15, 16, 11};
	private String name;
	private int durability;
	private int[] damageReductionAmountArray;
	private int enchantability;
	private String equipSound;
	private float toughness;
	private Item repairItem;
	
	private ArmorMaterialList(String nameIn, int durabilityIn, int[] damageReductionAmountsIn, int enchantabilityIn,
			String equipSoundIn, float toughnessIn, Item repairMaterialIn) {
		
		this.name = nameIn;
		this.durability = durabilityIn;
		this.damageReductionAmountArray = damageReductionAmountsIn;
		this.enchantability = enchantabilityIn;
		this.equipSound = equipSoundIn;
		this.toughness = toughnessIn;
		this.repairItem = repairMaterialIn;
	}
	
	@Override
	public String getName() {
		return TATsMod.MOD_ID + ":" + this.name;
	}
	@Override
	public int getDurability(EquipmentSlotType slotIn) {
		return MAX_DAMAGE_ARRAY[slotIn.getIndex()] * this.durability;
	}
	@Override
	public int getDamageReductionAmount(EquipmentSlotType slotIn) {
		return this.damageReductionAmountArray[slotIn.getIndex()];
	}
	@Override
	public int getEnchantability() {
		return this.enchantability;
	}
	@Override
	public SoundEvent getSoundEvent() {
		return new SoundEvent(TATsMod.nRL(equipSound));
	}
	@Override
	public float getToughness() {
		return this.toughness;
	}
	@Override
	public Ingredient getRepairMaterial() {
		return Ingredient.fromItems(this.repairItem);
	}
}