package com.tjtofvesson.mcoverhaul.block;

import java.util.List;

import com.tjtofvesson.mcoverhaul.Reference;

import net.minecraft.block.BlockPressurePlate;
import net.minecraft.block.material.Material;
import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.entity.Entity;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;
import net.minecraftforge.fml.relauncher.Side;
import net.minecraftforge.fml.relauncher.SideOnly;

public class PlayerPlate extends BlockPressurePlate{

	public PlayerPlate(){ this(null, null); }
	
	protected PlayerPlate(Material materialIn, Sensitivity sensitivityIn) {
		super(Material.ROCK, Sensitivity.MOBS);
		setUnlocalizedName(Reference.MCOBlocksData.PlayerPlate.getUnlocalizedName());
		setRegistryName(Reference.MCOBlocksData.PlayerPlate.getRegistryName());
		setCreativeTab(CreativeTabs.REDSTONE);
		slipperiness = -0.5f;
	}
	
	@Override
	protected int computeRedstoneStrength(World worldIn, BlockPos pos)
    {
		List<EntityPlayer> trigger = worldIn.<EntityPlayer>getEntitiesWithinAABB(EntityPlayer.class, PRESSURE_AABB.offset(pos));
		if(!trigger.isEmpty()) for(EntityPlayer e : trigger) if(!e.doesEntityNotTriggerPressurePlate()) return 15;
		return 0;
    }
	
	@SideOnly(Side.CLIENT)
    public CreativeTabs getCreativeTab()
    {
        return CreativeTabs.REDSTONE;
    }

}