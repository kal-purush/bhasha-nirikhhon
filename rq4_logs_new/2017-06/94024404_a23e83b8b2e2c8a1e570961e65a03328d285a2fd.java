package com.teambr.mako.api.tile;

import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTUtil;
import net.minecraft.util.EnumFacing;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;
import net.minecraftforge.common.capabilities.Capability;

import javax.annotation.Nullable;

public class TileEntitySimpleMultiblockComponent extends TileEntityBase {

    private BlockPos controller;

    public TileEntitySimpleMultiblockComponent(BlockPos controller, World world, BlockPos pos) {
        this.controller = controller;
        this.setWorld(world);
        this.setPos(pos);
    }

    @Override
    public NBTTagCompound writeToNBT(NBTTagCompound compound) {
        compound.setTag("controller", NBTUtil.createPosTag(controller));
        return super.writeToNBT(compound);
    }

    @Override
    public void readFromNBT(NBTTagCompound compound) {
        controller = null;
        if (compound.hasKey("controller")) controller = NBTUtil.getPosFromTag(compound.getCompoundTag("controller"));
        super.readFromNBT(compound);
    }

    @Override
    public boolean hasCapability(Capability<?> capability, @Nullable EnumFacing facing) {
        if (this.world.getTileEntity(controller) instanceof IHasExternalCapability)
            return ((IHasExternalCapability) this.world.getTileEntity(controller)).hasExternalCapability(this.pos, capability, facing);
        return super.hasCapability(capability, facing);
    }

    @Nullable
    @Override
    public <T> T getCapability(Capability<T> capability, @Nullable EnumFacing facing) {
        if (this.world.getTileEntity(controller) instanceof IHasExternalCapability)
            return ((IHasExternalCapability) this.world.getTileEntity(controller)).getExternalCapability(pos, capability, facing);
        return super.getCapability(capability, facing);
    }
}