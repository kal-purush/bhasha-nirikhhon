package com.teambr.mako.block;

import com.teambr.mako.api.mako.IMako;
import com.teambr.mako.api.mako.MakoRegistry;
import com.teambr.mako.api.mako.stack.MakoStack;
import com.teambr.mako.api.mako.stack.MakoTank;
import com.teambr.mako.tile.TileEntityPortableTank;
import net.minecraft.block.ITileEntityProvider;
import net.minecraft.block.state.IBlockState;
import net.minecraft.client.util.ITooltipFlag;
import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.entity.EntityLivingBase;
import net.minecraft.entity.item.EntityItem;
import net.minecraft.item.ItemBlock;
import net.minecraft.item.ItemStack;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.tileentity.TileEntity;
import net.minecraft.util.NonNullList;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.IBlockAccess;
import net.minecraft.world.World;
import net.minecraftforge.registries.IForgeRegistry;
import org.apache.commons.lang3.text.WordUtils;

import javax.annotation.Nullable;
import java.util.List;

public class PortableTankBlock extends MakoBlock implements ITileEntityProvider {

    public PortableTankBlock() {
        super("portable_tank");
    }

    @Nullable
    @Override
    public TileEntity createNewTileEntity(World worldIn, int meta) {
        return new TileEntityPortableTank();
    }

    @Override
    public void registerItem(IForgeRegistry registry) {
        registry.register(new ItemBlock(this).setRegistryName(this.getRegistryName()).setUnlocalizedName(this.getUnlocalizedName()).setMaxStackSize(1));
    }

    @Override
    public void breakBlock(World world, BlockPos pos, IBlockState state) {
        TileEntity entity = world.getTileEntity(pos);
        if (entity != null && entity instanceof TileEntityPortableTank) {
            ItemStack stack = new ItemStack(this, 1);
            stack.setTagCompound(new NBTTagCompound());
            TileEntityPortableTank tileEntityPortableTank = (TileEntityPortableTank) entity;
            if (tileEntityPortableTank.getTank().getMakoStack() != null) {
                NBTTagCompound compound = new NBTTagCompound();
                compound = tileEntityPortableTank.getTank().writeToNBT(compound);
                stack.getTagCompound().setTag("Tank", compound);
            }
            float f = 0.7F;
            float d0 = world.rand.nextFloat() * f + (1.0F - f) * 0.5F;
            float d1 = world.rand.nextFloat() * f + (1.0F - f) * 0.5F;
            float d2 = world.rand.nextFloat() * f + (1.0F - f) * 0.5F;
            EntityItem entityitem = new EntityItem(world, pos.getX() + d0, pos.getY() + d1, pos.getZ() + d2, stack);
            entityitem.setDefaultPickupDelay();
            if (stack.hasTagCompound()) {
                entityitem.getItem().setTagCompound(stack.getTagCompound().copy());
            }
            world.spawnEntity(entityitem);
        }
        super.breakBlock(world, pos, state);
    }

    @Override
    public void getSubBlocks(CreativeTabs itemIn, NonNullList<ItemStack> items) {
        super.getSubBlocks(itemIn, items);
        for (IMako mako : MakoRegistry.getInstance().getMakoRegistry().values()) {
            ItemStack stack = new ItemStack(this);
            MakoTank tank = new MakoTank(8000);
            tank.fill(new MakoStack(mako, 8000));
            stack.setTagCompound(new NBTTagCompound());
            stack.getTagCompound().setTag("Tank", tank.writeToNBT(new NBTTagCompound()));
            items.add(stack);
        }
    }

    @Override
    public void addInformation(ItemStack stack, @Nullable World player, List<String> tooltip, ITooltipFlag advanced) {
        super.addInformation(stack, player, tooltip, advanced);
        if (stack.hasTagCompound() && stack.getTagCompound().hasKey("Tank")) {
            NBTTagCompound compound = stack.getTagCompound().getCompoundTag("Tank");
            tooltip.add("Mako: " + WordUtils.capitalize(compound.getString("MakoName")));
            tooltip.add("Amount: " + compound.getInteger("Amount") + "mp");
        }
    }

    @Override
    public void onBlockPlacedBy(World worldIn, BlockPos pos, IBlockState state, EntityLivingBase placer, ItemStack stack) {
        super.onBlockPlacedBy(worldIn, pos, state, placer, stack);
        if (worldIn.getTileEntity(pos) != null && worldIn.getTileEntity(pos) instanceof TileEntityPortableTank) {
            TileEntityPortableTank tile = (TileEntityPortableTank) worldIn.getTileEntity(pos);
            if (stack.hasTagCompound() && stack.getTagCompound().hasKey("Tank")) {
                System.out.println(stack.getTagCompound());
                tile.setTank(tile.getTank().readFromNBT(stack.getTagCompound().getCompoundTag("Tank")));
            }
        }
    }

    @Override
    public void getDrops(NonNullList<ItemStack> drops, IBlockAccess world, BlockPos pos, IBlockState state, int fortune) {
    }
}