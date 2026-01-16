package com.teambr.mako.block;


import net.minecraft.block.properties.PropertyBool;
import net.minecraft.block.state.IBlockState;
import net.minecraft.client.renderer.block.model.ModelResourceLocation;
import net.minecraft.item.Item;
import net.minecraftforge.client.model.ModelLoader;


public class InvisibleMakoBlock extends MakoBlock {

    public static PropertyBool INVISIBLE = PropertyBool.create("invisible");

    public InvisibleMakoBlock(String name) {
        super(name);
        this.setDefaultState(this.blockState.getBaseState().withProperty(INVISIBLE, false));
    }

    @Override
    public IBlockState getStateFromMeta(int meta) {
        if (meta == 0) return this.getDefaultState().withProperty(INVISIBLE, false);
        if (meta == 1) return this.getDefaultState().withProperty(INVISIBLE, true);
        return super.getStateFromMeta(meta);
    }

    @Override
    public int getMetaFromState(IBlockState state) {
        return state.getValue(INVISIBLE) ? 1 : 0;
    }

    @Override
    public void registerRender() {
        ModelLoader.setCustomModelResourceLocation(Item.getItemFromBlock(this), 0, new ModelResourceLocation(this.getRegistryName().toString(), "invisible=false"));
        ModelLoader.setCustomModelResourceLocation(Item.getItemFromBlock(this), 1, new ModelResourceLocation(this.getRegistryName().toString(), "invisible=true"));
    }

}