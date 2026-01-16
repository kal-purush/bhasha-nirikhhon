package com.teambr.mako.api.mako.stack;

import net.minecraft.nbt.NBTTagCompound;
import net.minecraftforge.fluids.FluidStack;

import javax.annotation.Nullable;

public class MakoTank implements IMakoTank, IMakoHandler {

    @Nullable
    protected MakoStack mako; //TODO Set as a capability
    protected int capacity;

    public MakoTank(int capacity) {
        this(null, capacity);
    }

    public MakoTank(@Nullable MakoStack makoStack, int capacity) {
        this.mako = makoStack;
        this.capacity = capacity;
    }

    @Nullable
    @Override
    public FluidStack drain(MakoStack res, boolean doDrain) {
        return null;
    }

    @Override
    public MakoStack getMakoStack() {
        return mako;
    }

    @Override
    public int getMakoAmount() {
        return mako == null ? 0 : mako.getAmount();
    }

    @Override
    public int getMaxMakoAmount() {
        return capacity;
    }

    public void setMaxMakoAmount(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public MakoTankInfo getTankInfo() {
        return new MakoTankInfo(this);
    }

    @Override
    public int fill(MakoStack stack, boolean doFill) { //TODO Implement Fill & Drain methods
        return 0;
    }

    @Nullable
    @Override
    public FluidStack drain(int max, boolean doDrain) {
        return null;
    }

    public MakoTank readFromNBT(NBTTagCompound nbt) {
        if (!nbt.hasKey("Empty")) {
            mako = MakoStack.loadFromNBT(nbt);
        } else {
            mako = null;
        }
        return this;
    }

    public NBTTagCompound writeToNBT(NBTTagCompound nbt) {
        if (mako == null) {
            nbt.setString("Empty", "");
        } else {
            mako.writeToNBT(nbt);
        }
        return nbt;
    }
}