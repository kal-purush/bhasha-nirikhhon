package huige233.transcend.compat;

import huige233.transcend.Main;
import huige233.transcend.init.ModItems;
import huige233.transcend.util.IHasModel;
import huige233.transcend.util.ItemNBTHelper;
import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.item.ItemStack;
import net.minecraft.tileentity.TileEntity;
import net.minecraft.util.NonNullList;
import net.minecraftforge.fml.common.Optional;
import org.jetbrains.annotations.NotNull;
import vazkii.botania.api.mana.ICreativeManaProvider;
import vazkii.botania.api.mana.IManaItem;
import vazkii.botania.api.mana.IManaTooltipDisplay;

public class botaniasword implements ICreativeManaProvider, IManaItem, IManaTooltipDisplay {
    protected static final int MAX_MANA = Integer.MAX_VALUE;
    private static final String TAG_CREATIVE = "creative";
    private static final String TAG_ONE_USE = "oneUse";

    private static final String TAG_MANA = "mana";

    @Optional.Method(modid = "botania")
    public void getSubItems(@NotNull CreativeTabs tab, @NotNull NonNullList<ItemStack> stack) {
        if(tab == Main.TranscendTab) {
            ItemStack create = new ItemStack(ModItems.TRANSCEND_SWORD);
            setMana(create, MAX_MANA);
            isCreative(create);
            setStackCreative(create);
            stack.add(create);
        }
    }

    @Optional.Method(modid = "botania")
    public static void setMana(ItemStack stack, int mana) {
        ItemNBTHelper.setInt(stack, TAG_MANA, MAX_MANA-1);
    }

    @Optional.Method(modid = "botania")
    public static void setStackCreative(ItemStack stack) {
        ItemNBTHelper.setBoolean(stack, TAG_CREATIVE, true);
    }

    @Override
    @Optional.Method(modid = "botania")
    public int getMana(ItemStack stack) {
        return ItemNBTHelper.getInt(stack, TAG_MANA, 0);
    }

    @Override
    @Optional.Method(modid = "botania")
    public int getMaxMana(ItemStack stack) {
        return MAX_MANA-1;
    }

    @Override
    @Optional.Method(modid = "botania")
    public void addMana(ItemStack stack, int mana) {
        setMana(stack, Math.min(getMana(stack) + mana, getMaxMana(stack)));
    }

    @Override
    @Optional.Method(modid = "botania")
    public boolean canReceiveManaFromPool(ItemStack stack, TileEntity pool) {
        return !ItemNBTHelper.getBoolean(stack, TAG_ONE_USE, false);
    }

    @Override
    @Optional.Method(modid = "botania")
    public boolean canReceiveManaFromItem(ItemStack stack, ItemStack otherStack) {
        return true;
    }

    @Override
    @Optional.Method(modid = "botania")
    public boolean canExportManaToPool(ItemStack stack, TileEntity pool) {
        return true;
    }

    @Override
    @Optional.Method(modid = "botania")
    public boolean canExportManaToItem(ItemStack stack, ItemStack otherStack) {
        return true;
    }

    @Override
    @Optional.Method(modid = "botania")
    public boolean isNoExport(ItemStack stack) {
        return true;
    }

    @Override
    @Optional.Method(modid = "botania")
    public float getManaFractionForDisplay(ItemStack stack) {
        return (float) getMana(stack) / (float)getMaxMana(stack);
    }

    @Override
    @Optional.Method(modid = "botania")
    public boolean isCreative(ItemStack stack) {
        return false;
    }
}