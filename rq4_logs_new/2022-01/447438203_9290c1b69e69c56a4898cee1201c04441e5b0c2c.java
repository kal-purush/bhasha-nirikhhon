package com.tassaron.copperlamp.block;

import com.tassaron.copperlamp.CopperLampMod;
import com.tassaron.copperlamp.blockentity.PoweredCopperBlockEntity;
import net.minecraft.block.*;
import net.minecraft.block.entity.BlockEntity;
import net.minecraft.block.entity.BlockEntityTicker;
import net.minecraft.block.entity.BlockEntityType;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;

public class PoweredCopperBlock extends BlockWithEntity implements BlockEntityProvider {

    public PoweredCopperBlock(AbstractBlock.Settings settings) {
            super(settings);
    }

    @Override
    public BlockEntity createBlockEntity(BlockPos pos, BlockState state) {
        return new PoweredCopperBlockEntity(pos, state);
    }

    @Override
    public BlockRenderType getRenderType(BlockState state) {
        // With inheriting from BlockWithEntity this defaults to INVISIBLE, so we need to change that!
        return BlockRenderType.MODEL;
    }

    @Override
    public <T extends BlockEntity> BlockEntityTicker<T> getTicker(World world, BlockState state, BlockEntityType<T> type) {
        return checkType(type, CopperLampMod.POWERED_COPPER_BLOCK_ENTITY, (world1, pos, state1, be) -> PoweredCopperBlockEntity.tick(world1, pos, state1, be));
    }
}