package com.teambr.mako.block;

import com.teambr.mako.Mako;
import com.teambr.mako.api.block.IRegistrable;
import com.teambr.mako.utils.Reference;
import net.minecraft.block.Block;
import net.minecraft.block.material.Material;
import net.minecraft.client.renderer.block.model.ModelResourceLocation;
import net.minecraft.item.Item;
import net.minecraft.item.ItemBlock;
import net.minecraft.util.ResourceLocation;
import net.minecraftforge.client.model.ModelLoader;
import net.minecraftforge.fml.common.registry.GameRegistry;

public class MakoBlock extends Block implements IRegistrable {

    public MakoBlock(String name) {
        super(Material.ROCK);
        this.setRegistryName(new ResourceLocation(Reference.MODID, name));
        this.setUnlocalizedName(this.getRegistryName().toString());
        this.setCreativeTab(Mako.creativeTab);
    }

    @Override
    public void register() {
        GameRegistry.register(this);
        GameRegistry.register(new ItemBlock(this), this.getRegistryName());
    }

    @Override
    public void registerRender() {
        ModelLoader.setCustomModelResourceLocation(Item.getItemFromBlock(this), 0, new ModelResourceLocation(this.getRegistryName().toString(), "normal"));
    }
}