package net.shadowfacts.enfusion.machine.router;

import cpw.mods.fml.common.network.internal.FMLNetworkHandler;
import cpw.mods.fml.relauncher.Side;
import cpw.mods.fml.relauncher.SideOnly;
import net.minecraft.block.Block;
import net.minecraft.block.material.Material;
import net.minecraft.client.renderer.texture.IIconRegister;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.item.Item;
import net.minecraft.tileentity.TileEntity;
import net.minecraft.util.IIcon;
import net.minecraft.world.World;
import net.minecraftforge.common.util.ForgeDirection;
import net.shadowfacts.enfusion.EnFusion;
import net.shadowfacts.enfusion.block.EFBlocks;
import net.shadowfacts.enfusion.client.gui.GUIs;
import net.shadowfacts.shadowcore.block.BaseContainerBlock;

/**
 * Block class for the router
 * @author shadowfacts
 */
public class BlockRouter extends BaseContainerBlock {

	@SideOnly(Side.CLIENT)
	private IIcon iconTop;

	public BlockRouter(String name) {
		super(name, Material.rock);
		this.setBlockName(name);
	}

	@SideOnly(Side.CLIENT)
	public Item getItem() {
		return Item.getItemFromBlock(EFBlocks.router);
	}

	@SideOnly(Side.CLIENT)
	@Override
	public void registerBlockIcons(IIconRegister iconRegister) {
		this.blockIcon = iconRegister.registerIcon(EnFusion.modId + ":machine/router/side");
		this.iconTop = iconRegister.registerIcon(EnFusion.modId + ":machine/router/top");
	}

	@SideOnly(Side.CLIENT)
	@Override
	public IIcon getIcon(int side, int metadata) {
		if (ForgeDirection.getOrientation(side) == ForgeDirection.UP || ForgeDirection.getOrientation(side) == ForgeDirection.DOWN) {
			return this.iconTop;
		} else {
			return this.blockIcon;
		}
	}

	@Override
	public boolean onBlockActivated(World world, int x, int y, int z, EntityPlayer player, int par6, float par7, float par8, float par9) {
		if (!world.isRemote) {
			FMLNetworkHandler.openGui(player, EnFusion.instance, GUIs.ROUTER, world, x, y, z);
		}
		return true;
	}

	@Override
	public TileEntity createNewTileEntity(World world, int metadata) {
		return new TileEntityRouter();
	}

	@Override
	public void breakBlock(World world, int x, int y, int z, Block block, int par6) {

	}

}