package net.shadowfacts.enfusion.machine.router;

import net.minecraft.item.ItemStack;
import net.shadowfacts.shadowcore.tileentity.BaseModTileEntity;

/**
 * Tile Entity for the router.
 * @author shadowfacts
 */
//public class TileEntityRouter extends BaseModTileEntity implements IInventory {
public class TileEntityRouter extends BaseModTileEntity {

	private ItemStack routerStack;

	public int getSizeInventory() {
		return 0;
	}

	public ItemStack getStackInSlot(int slot) {
		if (slot == 0) {
			return routerStack;
		} else {
			return null;
		}
	}

	public ItemStack decrStackSize(int slot, int amount) {
//		if (slot == 0 && routerStack != null) {
//
//		}
		return routerStack;
	}

}