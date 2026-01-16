package com.felipendelicia.boylemod.item;

import net.minecraft.item.ItemGroup;
import net.minecraft.item.ItemStack;

public class ModItemGroup {
    public static final ItemGroup BOYLE_GROUP_ITEMS = new ItemGroup("BoyleMod:Items") {
        @Override
        public ItemStack createIcon() {
            return new ItemStack(ModItems.COBALT_INGOT.get());
        }
    };
}