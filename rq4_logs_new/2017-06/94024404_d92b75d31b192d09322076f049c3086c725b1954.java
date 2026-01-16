package com.teambr.mako.api.tile;

import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.inventory.Container;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.network.NetworkManager;
import net.minecraft.network.play.server.SPacketUpdateTileEntity;
import net.minecraft.tileentity.TileEntity;
import net.minecraft.util.ITickable;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;
import net.minecraftforge.common.util.INBTSerializable;

import javax.annotation.Nullable;
import java.util.HashMap;

public class TileEntityBase extends TileEntity implements ITickable, IHasGui {

    private HashMap<String, INBTSerializable<NBTTagCompound>> nbtStorage;

    public TileEntityBase() {
        nbtStorage = new HashMap<>();
    }

    @Nullable//DIRectional tile
    @Override
    public SPacketUpdateTileEntity getUpdatePacket() {
        NBTTagCompound nbt = new NBTTagCompound();
        this.writeToNBT(nbt);
        return new SPacketUpdateTileEntity(this.pos, 3, nbt);
    }

    @Override
    public void onDataPacket(NetworkManager net, SPacketUpdateTileEntity pkt) {
        this.readFromNBT(pkt.getNbtCompound());
    }

    @Override
    public NBTTagCompound writeToNBT(NBTTagCompound compound) {
        nbtStorage.forEach((s, nbtTagCompoundINBTSerializable) -> compound.setTag(s, nbtTagCompoundINBTSerializable.serializeNBT()));
        return compound;
    }

    @Override
    public void readFromNBT(NBTTagCompound compound) {
        nbtStorage.forEach((s, nbtTagCompoundINBTSerializable) -> {
            if (compound.hasKey(s)) nbtTagCompoundINBTSerializable.deserializeNBT(compound.getCompoundTag(s));
        });
    }

    @Override
    public void update() {
        if (world.isRemote) return;

    }


    @Override
    public Container getClientGUI(int id, EntityPlayer player, World world, BlockPos pos) {
        return null;
    }

    public void addNBTToStorage(String name, INBTSerializable<NBTTagCompound> nbtable) {
        nbtStorage.put(name, nbtable);
    }
}