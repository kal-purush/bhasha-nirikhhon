package com.teambr.mako.api.mako.stack;

import com.teambr.mako.Mako;
import com.teambr.mako.api.mako.IMako;
import com.teambr.mako.api.mako.MakoRegistry;
import jline.internal.Nullable;
import net.minecraft.nbt.NBTTagCompound;
import org.apache.logging.log4j.Level;

public class MakoStack {

    private int amount;
    private IMako mako;

    public MakoStack(IMako mako, int amount) {
        if (mako == null) {
            Mako.LOGGER.log(Level.ERROR, "Mako supplied to the MakoStack was null.");
            throw new IllegalArgumentException("Cannot create a MakoStack from a null Mako.");
        }
        if (MakoRegistry.getInstance().getMako(mako.getName()) == null) {
            Mako.LOGGER.log(Level.ERROR, "Mako supplied to the MakoStack was not registered into the MakoRegistry.");
            throw new IllegalArgumentException("Cannot create a MakoStack from an unregistered Mako.");
        }
        this.amount = amount;
        this.mako = mako;
    }

    public MakoStack(MakoStack stack, int amount) {
        this(stack.getMako(), amount);
    }

    public int getAmount() {
        return amount;
    }

    public IMako getMako() {
        return mako;
    }

    public NBTTagCompound writeToNBT(NBTTagCompound nbt) {
        nbt.setString("MakoName", mako.getName());
        nbt.setInteger("Amount", amount);
        return nbt;
    }

    @Nullable
    public static MakoStack loadFromNBT(NBTTagCompound nbt) {
        if (nbt == null) return null;
        if (!nbt.hasKey("MakoName")) return null;
        String name = nbt.getString("MakoName");
        IMako mako = MakoRegistry.getInstance().getMako(name);
        if (mako == null) return null;
        return new MakoStack(mako, nbt.getInteger("Amount"));
    }

    public boolean isMakoEqual(MakoStack other) {
        return this.mako.equals(other.getMako());
    }

}