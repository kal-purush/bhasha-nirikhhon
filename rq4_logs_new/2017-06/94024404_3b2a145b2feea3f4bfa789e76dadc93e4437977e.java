package com.teambr.mako.multiblock;

import com.teambr.mako.api.multiblock.MachineMultiblock;
import com.teambr.mako.proxy.CommonProxy;
import net.minecraft.block.Block;
import net.minecraft.init.Blocks;
import net.minecraft.item.ItemStack;
import net.minecraft.util.math.BlockPos;

public class InfuserMultiblock extends MachineMultiblock {

    private static ItemStack[][][] structure = new ItemStack[2][2][1];
    public static InfuserMultiblock INFUSER_MULTIBLOCK = new InfuserMultiblock("infuser", structure, new BlockPos(0, 1, 0));

    static {
        structure[0][0][0] = new ItemStack(Blocks.IRON_BLOCK);
        structure[0][1][0] = new ItemStack((Block) CommonProxy.blockRegistry.get("infuser_controller"));
        structure[1][0][0] = new ItemStack(Blocks.IRON_BLOCK);
        structure[1][1][0] = new ItemStack(Blocks.IRON_BLOCK);
    }

    public InfuserMultiblock(String name, ItemStack[][][] multiblock, BlockPos controller) {
        super(name, multiblock, controller);
    }
}