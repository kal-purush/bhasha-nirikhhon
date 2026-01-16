package com.tassaron.copperlamp.block;

import com.tassaron.copperlamp.CopperLampMod;
import net.fabricmc.fabric.api.transfer.v1.transaction.Transaction;
import net.minecraft.block.BlockState;
import net.minecraft.block.RedstoneTorchBlock;
import net.minecraft.server.world.ServerWorld;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.math.Direction;
import net.minecraft.world.World;
import team.reborn.energy.api.EnergyStorage;
import team.reborn.energy.api.base.SimpleEnergyStorage;

import java.util.Random;

public class CopperTorchBlock extends RedstoneTorchBlock {
    private short tickDelay = 20;
    public CopperTorchBlock(Settings settings) {
        super(settings);
    }

    public void onBlockAdded(BlockState state, World world, BlockPos pos, BlockState oldState, boolean notify) {
        super.onBlockAdded(state, world, pos, oldState, notify);
        world.createAndScheduleBlockTick(pos, world.getBlockState(pos).getBlock(), tickDelay);
    }

    public void scheduledTick(BlockState state, ServerWorld world, BlockPos pos, Random random) {
        super.scheduledTick(state, world, pos, random);
        if (!(Boolean)state.get(LIT)) {
            // Burnt out torches are handled by the parent class
            return;
        }
        world.createAndScheduleBlockTick(pos, world.getBlockState(pos).getBlock(), tickDelay);
        if (world.isClient) {
            // energy transfer is server-side
            return;
        }
        EnergyStorage es = SimpleEnergyStorage.SIDED.find(world, pos.up(), Direction.DOWN);
        if (es == null) {
            return;
        }
        try (Transaction transaction = Transaction.openOuter()) {
            long amountInserted = es.insert(1, transaction);
            if (amountInserted == 1) {
                CopperLampMod.LOGGER.info("Copper torch is transmitting power to x{}, y{}, z{}", pos.getX(), pos.getY(), pos.getZ());
                transaction.commit();
            }
        }
    }
}