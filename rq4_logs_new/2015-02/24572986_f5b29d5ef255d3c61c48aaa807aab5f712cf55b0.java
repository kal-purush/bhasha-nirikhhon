package net.shadowfacts.enfusion.block.misc;

import net.minecraft.block.material.Material;
import net.minecraft.entity.Entity;
import net.minecraft.world.World;
import net.shadowfacts.enfusion.block.EFBlock;

/**
 * Concrete
 * @author shadowdfacts
 */
public class BlockConcrete extends EFBlock {

	public BlockConcrete() {
		super(Material.rock);
	}

	@Override
	public void onEntityWalking(World world, int x, int y, int z, Entity entity) {
		entity.motionX *= 2;
		entity.motionZ *= 3;
//		entity.stepHeight = 1f;
//		TODO: change step height back to 0.5f after someone stops walking on the concrete
	}

}