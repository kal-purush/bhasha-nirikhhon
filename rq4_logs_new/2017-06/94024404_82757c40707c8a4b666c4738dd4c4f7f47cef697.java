package com.teambr.mako.world;

import com.teambr.mako.api.mako.MakoRegistry;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.util.math.ChunkPos;
import net.minecraftforge.event.world.ChunkDataEvent;
import net.minecraftforge.event.world.ChunkEvent;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;

import java.util.HashMap;
import java.util.Random;

public class GeiserChunkManager {

    private static GeiserChunkManager ourInstance = new GeiserChunkManager();

    public static GeiserChunkManager getInstance() {
        return ourInstance;
    }

    private HashMap<ChunkPos, GeiserData> chunkGeiserData;
    private Random random;

    private GeiserChunkManager() {
        chunkGeiserData = new HashMap<>();
        random = new Random(); //TODO Use a better random
    }

    @SubscribeEvent
    public void chunkLoad(ChunkDataEvent.Load event) {
        if (!event.getWorld().isRemote && !chunkGeiserData.containsKey(event.getChunk().getPos())) {
            if (event.getData().hasKey("Geiser")) {
                NBTTagCompound compound = event.getData().getCompoundTag("Geiser");
                if (!compound.hasKey("Empty")) {
                    chunkGeiserData.put(event.getChunk().getPos(), GeiserData.readFromNBT(compound));
                    System.out.println("Loaded Geiser data from " + event.getChunk().getPos() + " -> " + chunkGeiserData.get(event.getChunk().getPos()).toString());
                }
            } else {
                float f = random.nextFloat();
                System.out.println(f);
                if (random.nextFloat() < 0.1D) {
                    chunkGeiserData.put(event.getChunk().getPos(), new GeiserData(MakoRegistry.getInstance().getRandomPureMako(), 0, 0));
                    System.out.println("Generated Geiser data from " + event.getChunk().getPos() + " -> " + chunkGeiserData.get(event.getChunk().getPos()).toString());
                } else {
                    chunkGeiserData.put(event.getChunk().getPos(), null);
                }
            }
        }
    }

    @SubscribeEvent
    public void chunkSave(ChunkDataEvent.Save event) {
        if (!event.getWorld().isRemote && chunkGeiserData.containsKey(event.getChunk().getPos())) {
            if (chunkGeiserData.get(event.getChunk().getPos()) == null) {
                NBTTagCompound compound = new NBTTagCompound();
                compound.setString("Empty", "");
                event.getData().setTag("Geiser", compound);
                chunkGeiserData.remove(event.getChunk().getPos());
                return;
            }
            event.getData().setTag("Geiser", chunkGeiserData.get(event.getChunk().getPos()).writeFromNBT());
            System.out.println("Saving Geiser data from " + event.getChunk().getPos() + " -> " + chunkGeiserData.get(event.getChunk().getPos()).toString());
        }
    }

    @SubscribeEvent
    public void chunkUnload(ChunkEvent.Unload event) {
        if (!event.getWorld().isRemote && chunkGeiserData.containsKey(event.getChunk().getPos())) {
            System.out.println("Removing Geiser data from " + event.getChunk().getPos() + " -> " + chunkGeiserData.get(event.getChunk().getPos()).toString());
            chunkGeiserData.remove(event.getChunk().getPos());
        }
    }
}