package net.elreyjota.revenantmod.util;

import net.elreyjota.revenantmod.RevenantMod;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.tags.BlockTags;
import net.minecraft.tags.ItemTags;
import net.minecraft.tags.TagKey;
import net.minecraft.world.item.Item;
import net.minecraft.world.level.block.Block;

public class ModTags {
    public static class Blocks{

        // List of tags

        // Tag Creator
        private static TagKey<Block> tag(String name) {
            return BlockTags.create(new ResourceLocation(RevenantMod.MOD_ID, name));
        }
    }

    public static class Items {

        // List of tags

        // Tag Creator
        private static TagKey<Item> tag(String name){
            return ItemTags.create(new ResourceLocation(RevenantMod.MOD_ID, name));
        }

    }

}