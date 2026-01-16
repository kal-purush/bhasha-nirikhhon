package fr.iglee42.techresourcecrystal.customize.datageneration;

import fr.iglee42.techresourcecrystal.customize.TypesConstants;
import fr.iglee42.techresourcecrystal.init.ModBlock;
import fr.iglee42.techresourcecrystal.init.ModItem;
import fr.iglee42.techresourcecrystal.init.ModTags;
import net.minecraft.core.Registry;
import net.minecraft.data.DataGenerator;
import net.minecraft.data.tags.BlockTagsProvider;
import net.minecraft.data.tags.ItemTagsProvider;
import net.minecraft.data.tags.TagsProvider;
import net.minecraft.tags.TagKey;
import net.minecraft.world.item.Item;
import net.minecraftforge.common.Tags;
import net.minecraftforge.common.data.ExistingFileHelper;
import net.minecraftforge.common.data.ForgeItemTagsProvider;
import org.jetbrains.annotations.Nullable;

public class CustomsTagsProvider {
    public static class Items extends ItemTagsProvider {
        public Items(DataGenerator p_126530_, BlockTagsProvider p_126531_, String modId, @Nullable ExistingFileHelper existingFileHelper) {
            super(p_126530_, p_126531_, modId, existingFileHelper);
        }

        @Override
        public void addTags() {
            TypesConstants.TYPES.forEach(c->{
                this.tag(ModTags.Items.CRYSTALS).add(ModItem.getCrystal(c.name()));
                this.tag(ModTags.Items.FRAGMENTED_CRYSTALS).add(ModItem.getFragmentedCrystal(c.name()));
                this.tag(ModTags.Items.PLATES).add(ModItem.getPlate(c.name()));
            });
        }
    }
    public static class Blocks extends BlockTagsProvider{
        public Blocks(DataGenerator p_126511_, String modId, @Nullable ExistingFileHelper existingFileHelper) {
            super(p_126511_, modId, existingFileHelper);
        }

        @Override
        public void addTags() {
            TypesConstants.TYPES.forEach(c->{
                this.tag(ModTags.Blocks.MINEABLE_PICKAXE).add(ModBlock.getCrystalCore(c.name())).add(ModBlock.getFragmentedCrystal(c.name()));
            });
        }
    }
}