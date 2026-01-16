package com.teammetallurgy.atum.world.biome;

import com.teammetallurgy.atum.blocks.AtumBlocks;
import com.teammetallurgy.atum.entity.*;
import com.teammetallurgy.atum.handler.AtumConfig;
import com.teammetallurgy.atum.world.decorators.*;

import net.minecraft.block.Block;
import net.minecraft.init.Blocks;
import net.minecraft.world.World;
import net.minecraft.world.biome.BiomeDecorator;
import net.minecraft.world.biome.BiomeGenBase;
import net.minecraft.world.gen.feature.WorldGenerator;

import java.util.Random;

public class BiomeGenLimestoneMountains extends AtumBiomeGenBase {

    public BiomeGenLimestoneMountains(AtumConfig.BiomeConfig config) {
        super(config);
        
        super.setHeight(height_MidHills);
        
        super.palmRarity = -1;
        // TODO: dead trees
        
        super.addDefaultSpawns();
    }
    
    public void genTerrainBlocks(World world, Random rng, Block[] blocks, byte[] bytes, int x, int z, double elevation)
    {
        super.topBlock = AtumBlocks.BLOCK_SAND;
        super.fillerBlock = AtumBlocks.BLOCK_STONE;

        if (elevation > 1.0D) {
            super.topBlock = AtumBlocks.BLOCK_STONE;
            super.fillerBlock = AtumBlocks.BLOCK_STONE;
        }

        this.genBiomeTerrain(world, rng, blocks, bytes, x, z, elevation);
    }

}