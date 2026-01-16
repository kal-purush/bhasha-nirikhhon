package com.yaricraft.equinemagic.block;

import com.yaricraft.equinemagic.creativetab.CreativeTabEquineMagic;
import com.yaricraft.equinemagic.enums.EEquineFoci;
import com.yaricraft.equinemagic.enums.EEquineGem;
import com.yaricraft.equinemagic.reference.MCData;
import com.yaricraft.equinemagic.reference.ModData;
import com.yaricraft.equinemagic.reference.ModNames;
import cpw.mods.fml.relauncher.Side;
import cpw.mods.fml.relauncher.SideOnly;
import net.minecraft.block.material.Material;
import net.minecraft.client.renderer.texture.IIconRegister;
import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.item.Item;
import net.minecraft.item.ItemStack;
import net.minecraft.util.IIcon;
import net.minecraft.world.World;

import java.util.List;
import java.util.Random;

/**
 * Created by Yari on 12/6/2014.
 */
public class BlockCompressed extends EquineMagicBlock
{
    public BlockCompressed()
    {
        super(Material.iron);
        this.setBlockName(ModNames.BLOCK_COMPRESSED);
        this.setCreativeTab(CreativeTabEquineMagic.tabEquineMagic);
        for (int i = 0; i < EEquineGem.values().length; i++ ) this.subNames[i] = EEquineGem.values()[i].toString();
        setHardness(3.0F);
        for (EEquineGem ore: EEquineGem.values())
        {
            switch (ore)
            {
                case OPAL:
                case BLACK_OPAL:
                case CHROMA:
                case ZIRCON:
                case HAFNIUM:
                case DOLOMITE:
                case SPECTRA:
                    setHarvestLevel("pickaxe", 2, ore.ordinal()); // Iron
                    break;
            }
        }
    }

    @Override
    public EEquineFoci getFoci(int meta)
    {
        switch (EEquineGem.values()[meta])
        {
            case SPECTRA:
                return EEquineFoci.PEGASUS;
            case BLACK_OPAL:
                return EEquineFoci.SHADOW;
            case OPAL:
            case CHROMA:
            case ZIRCON:
            case HAFNIUM:
            case DOLOMITE:
            default:
                return EEquineFoci.ELEMENTAL;
        }
    }

    @Override
    public int getDamageValue(World world, int x, int y, int z)
    {
        return world.getBlockMetadata(x, y, z);
    }

    @Override
    public int quantityDropped(int meta, int fortune, Random random) { return 1; }

    @SideOnly(Side.CLIENT)
    private IIcon[] icons;

    @Override
    @SideOnly(Side.CLIENT)
    public void registerBlockIcons(IIconRegister iconRegister)
    {
        icons = new IIcon[MCData.THREE_SIDED * MCData.BLOCK_META_MAX];

        for (int i = 0; i < icons.length / MCData.THREE_SIDED; i++)
        {
            for(int j = 0; j < MCData.THREE_SIDED; j++)
            {
                icons[i * MCData.THREE_SIDED + j] = iconRegister.registerIcon(this.getUnwrappedUnlocalizedName() + ModData.ASSET_SPACER + subNames[i]);
            }
        }
    }

    @Override
    @SideOnly(Side.CLIENT)
    public IIcon getIcon(int side, int meta)
    {
        if(side>2) side = 2;
        return icons[meta * MCData.THREE_SIDED + side];
    }

    @Override
    @SideOnly(Side.CLIENT)
    public void getSubBlocks(Item item, CreativeTabs tabs, List par3List)
    {
        for (int i = 0; i < EEquineGem.values().length; i++)
        {
            par3List.add(new ItemStack(item, 1, i));
        }
    }
}