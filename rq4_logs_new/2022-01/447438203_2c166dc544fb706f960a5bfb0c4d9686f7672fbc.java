package com.tassaron.copperlamp.block;

import net.minecraft.block.BlockState;
import net.minecraft.block.WallTorchBlock;
import net.minecraft.particle.DefaultParticleType;
import net.minecraft.server.world.ServerWorld;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;

import java.util.Random;

import static com.tassaron.copperlamp.block.CopperTorchBlock.transmitPowerUp;

public class WallCopperTorchBlock extends WallTorchBlock {
    private final short tickDelay = 20;

    public WallCopperTorchBlock(Settings settings, DefaultParticleType particle) {
        super(settings, particle);
    }

    public void onBlockAdded(BlockState state, World world, BlockPos pos, BlockState oldState, boolean notify) {
        super.onBlockAdded(state, world, pos, oldState, notify);
        world.createAndScheduleBlockTick(pos, world.getBlockState(pos).getBlock(), tickDelay);
    }

    public void scheduledTick(BlockState state, ServerWorld world, BlockPos pos, Random random) {
        super.scheduledTick(state, world, pos, random);
        world.createAndScheduleBlockTick(pos, world.getBlockState(pos).getBlock(), tickDelay);
        transmitPowerUp(world, pos);
    }
}