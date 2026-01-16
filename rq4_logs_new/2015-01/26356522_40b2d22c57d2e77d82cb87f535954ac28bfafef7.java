package minechemV6Base.item;

import minechemV6Base.fluid.ChemicalFluid;
import net.minecraft.item.Item;
import net.minecraft.item.ItemStack;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraftforge.fluids.FluidRegistry;
import net.minecraftforge.fluids.FluidStack;
import net.minecraftforge.fluids.IFluidContainerItem;

public class FluidContainerStackable extends Item implements IFluidContainerItem
{
    private final int capacity;
    private final int stackCapacity;

    public FluidContainerStackable(int capacity)
    {
        this(capacity,64);
    }

    public FluidContainerStackable(int capacity, int maxStackSize)
    {
        this.capacity = capacity;
        this.stackCapacity = capacity * maxStackSize;
        this.setMaxStackSize(maxStackSize);
    }

    @Override
    public FluidStack getFluid(ItemStack container)
    {
        NBTTagCompound tagCompound = container.stackTagCompound;
        if (tagCompound == null || !tagCompound.hasKey("FluidName"))
        {
            return null;
        }
        String fluidName = tagCompound.getString("FluidName");
        if (fluidName ==null || FluidRegistry.getFluid(fluidName)== null)
        {
            return null;
        }
        FluidStack result = new FluidStack(FluidRegistry.getFluidID(fluidName), container.stackSize * capacity);
        return result;
    }

    @Override
    public int getCapacity(ItemStack container)
    {
        return stackCapacity;
    }

    @Override
    public int fill(ItemStack container, FluidStack resource, boolean doFill)
    {
        if (resource == null || !(resource.getFluid() instanceof ChemicalFluid))
        {
            return 0;
        }

        int amountToFill = roundDownAmount(resource.amount);
        FluidStack fluidStack = getFluid(container);

        if (!doFill)
        {

            if (fluidStack == null)
            {
                return Math.min(stackCapacity, amountToFill);
            }

            if (!fluidStack.isFluidEqual(resource))
            {
                return 0;
            }

            return Math.min(capacity - fluidStack.amount, amountToFill);
        }

        if (container.stackTagCompound == null)
        {
            container.stackTagCompound = new NBTTagCompound();
        }

        if (fluidStack == null)
        {
            amountToFill = Math.min(amountToFill,stackCapacity);
            setFluid(container, new FluidStack(resource.fluidID,amountToFill));
            return amountToFill;
        }

        NBTTagCompound fluidTag = container.stackTagCompound.getCompoundTag("Fluid");

        if (!fluidStack.isFluidEqual(resource))
        {
            return 0;
        }

        int filled = stackCapacity - fluidStack.amount;
        if (amountToFill < filled)
        {
            fluidStack.amount += amountToFill;
            filled = amountToFill;
        }
        else
        {
            fluidStack.amount = stackCapacity;
        }
        setFluid(container,fluidStack);
        return filled;
    }

    @Override
    public FluidStack drain(ItemStack container, int maxDrain, boolean doDrain)
    {
        FluidStack fluidStack = getFluid(container);
        if (fluidStack == null)
        {
            return null;
        }

        int currentAmount = fluidStack.amount;
        fluidStack.amount = Math.min(currentAmount, roundDownAmount(maxDrain));
        if (doDrain)
        {
            if (currentAmount == fluidStack.amount)
            {
                container.stackTagCompound.removeTag("FluidName");

                if (container.stackTagCompound.hasNoTags())
                {
                    container.stackTagCompound = null;
                }
                return fluidStack;
            }
            setFluid(container, new FluidStack(fluidStack.fluidID, currentAmount - fluidStack.amount));
        }
        return fluidStack;
    }

    private ItemStack setFluid(ItemStack container, FluidStack fluid)
    {
        if (container==null || fluid==null || fluid.amount<=0)
        {
            return null;
        }
        container.stackSize = fluid.amount/capacity;
        container.stackTagCompound = getNBT(fluid);
        return container;
    }

    private int roundDownAmount(int amount)
    {
        return amount - (amount % capacity);
    }

    private static NBTTagCompound getNBT(FluidStack fluid)
    {
        NBTTagCompound result = new NBTTagCompound();
        result.setString("FluidName", FluidRegistry.getFluidName(fluid.fluidID));
        if (fluid.tag != null)
        {
            result.setTag("Tag", fluid.tag);
        }
        return result;
    }
}