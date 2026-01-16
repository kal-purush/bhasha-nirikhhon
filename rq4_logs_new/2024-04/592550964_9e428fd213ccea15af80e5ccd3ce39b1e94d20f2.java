package io.lcydev.spawnercleaner.handlers;

import org.bukkit.Chunk;

public class ChunkManager {

    public static boolean initChunk(Chunk chunk) {
        if (!chunk.isLoaded() && !chunk.load()) return false;

        ChunkWrapper chunkWrapper = new ChunKWrapper;
        int[] chunkCoords = chunk.

    }
}