package huige233.transcend.tileEntity;

import net.minecraft.tileentity.TileEntity;
import net.minecraft.util.EnumFacing;
import net.minecraft.util.ITickable;
import net.minecraft.util.math.BlockPos;
import net.minecraftforge.common.capabilities.Capability;
import net.minecraftforge.energy.CapabilityEnergy;
import net.minecraftforge.energy.IEnergyStorage;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static net.minecraftforge.energy.CapabilityEnergy.ENERGY;

public class TileCreativeRFSource extends TileEntity implements ITickable, IEnergyStorage {
    @Override
    public void update() {
        if(getWorld().isRemote){
            return;
        }
        BlockPos pos = this.getPos();
        for(EnumFacing facing : EnumFacing.VALUES){
            TileEntity tile = world.getTileEntity(this.getPos().offset(facing));
            if(tile != null && tile.hasCapability(ENERGY,facing.getOpposite())){
                IEnergyStorage storage = tile.getCapability(ENERGY,facing.getOpposite());
                if(storage != null){
                    for(int i = 0 ; i <127;i++) {
                        this.receiveEnergy(storage.receiveEnergy(Integer.MAX_VALUE, false), false);
                    }
                }
            }
        }
    }

    @Override
    @Nullable
    public <T> T getCapability(@Nonnull Capability<T> capability, @Nullable EnumFacing facing) {
        if (capability == ENERGY) {
            return (T) this;
        }
        return super.getCapability(capability, facing);
    }

    @Override
    public boolean hasCapability(@Nonnull Capability<?> capability, @Nullable EnumFacing facing) {
        return capability == ENERGY || super.hasCapability(capability, facing);
    }

    @Override
    public int receiveEnergy(int maxReceive, boolean simulate) {
        return 0;
    }

    @Override
    public int extractEnergy(int maxExtract, boolean simulate) {
        return maxExtract;
    }

    @Override
    public int getEnergyStored() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxEnergyStored() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean canExtract() {
        return true;
    }

    @Override
    public boolean canReceive() {
        return false;
    }

}