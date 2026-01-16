package com.teambr.mako.tile;


import com.teambr.mako.api.tile.TileEntityMultiblock;
import net.minecraft.util.EnumFacing;
import net.minecraft.util.math.BlockPos;
import net.minecraftforge.common.capabilities.Capability;

import javax.annotation.Nullable;

public class TileEntityInfuser extends TileEntityMultiblock {

    public TileEntityInfuser() {
        super();
    }

    @Override
    public boolean hasExternalCapability(BlockPos pos, Capability<?> capability, @Nullable EnumFacing facing) {
        return false;
    }

    @Override
    public <T> T getExternalCapability(BlockPos pos, Capability<T> capability, @Nullable EnumFacing facing) {
        return null;
    }
}