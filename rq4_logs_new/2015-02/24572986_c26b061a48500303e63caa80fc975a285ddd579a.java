package net.shadowfacts.enfusion.block.misc;

import net.minecraft.block.Block;
import net.minecraft.init.Blocks;
import net.minecraft.item.Item;
import net.minecraft.world.World;
import net.minecraftforge.common.util.ForgeDirection;
import net.shadowfacts.enfusion.block.BlockOre;
import net.shadowfacts.enfusion.block.EFBlocks;
import net.shadowfacts.enfusion.item.EFItems;

import java.util.Random;

/**
 * Peridot ore
 * @author shadowfacts
 */
public class BlockPeridotOre extends BlockOre {

	@Override
	public Item getItemDropped(int par1, Random random, int par3) {
		return EFItems.gemPeridot;
	}

	@Override
	public int quantityDropped(int meta, int fortune, Random random) {
		return 1 + Math.min(fortune, random.nextInt(3));
	}

	@Override
	public void onPostBlockPlaced(World world, int x, int y, int z, int metadata) {
		for (ForgeDirection dir : ForgeDirection.VALID_DIRECTIONS) {
			int newX = x + dir.offsetX;
			int newY = y + dir.offsetY;
			int newZ = z + dir.offsetZ;
			if (world.getBlock(newX, newY, newZ).getUnlocalizedName().equals(Blocks.iron_ore.getUnlocalizedName())) {
				world.setBlock(newX, newY, newZ, EFBlocks.oreZinchorium);
				world.setBlock(x, y, z, EFBlocks.oreZinchorium);
			}
		}
	}

	@Override
	public void onNeighborBlockChange(World world, int x, int y, int z, Block block) {
		for (ForgeDirection dir : ForgeDirection.VALID_DIRECTIONS) {
			int newX = x + dir.offsetX;
			int newY = y + dir.offsetY;
			int newZ = z + dir.offsetZ;
			if (world.getBlock(newX, newY, newZ).getUnlocalizedName().equals(Blocks.iron_ore.getUnlocalizedName())) {
				world.setBlock(newX, newY, newZ, EFBlocks.oreZinchorium);
				world.setBlock(x, y, z, EFBlocks.oreZinchorium);
			}
		}
	}

}