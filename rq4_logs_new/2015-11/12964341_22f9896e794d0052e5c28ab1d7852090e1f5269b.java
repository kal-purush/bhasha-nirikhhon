package com.teammetallurgy.atum.world.biome;

import java.util.Random;

import com.teammetallurgy.atum.blocks.AtumBlocks;
import com.teammetallurgy.atum.handler.AtumConfig;

import net.minecraft.block.Block;
import net.minecraft.block.material.Material;
import net.minecraft.init.Blocks;
import net.minecraft.world.World;

public class BiomeGenOasis extends AtumBiomeGenBase {

    public BiomeGenOasis(AtumConfig.BiomeConfig config) {
        super(config);
        
        //super.topBlock = Blocks.grass;
        super.topBlock = AtumBlocks.BLOCK_FERTILESOIL;
        
        super.setHeight(height_PartiallySubmerged);
        super.setTemperatureRainfall(0.8F, 0.9F);
        
        // no hostile spawns here
        
        super.theBiomeDecorator.reedsPerChunk = 10;
        super.theBiomeDecorator.clayPerChunk = 1;
        super.theBiomeDecorator.waterlilyPerChunk = 2;
        
        super.palmRarity = 3;
    }
    
    // this is not working at ALL - we're missing something
    
    @Override
    public void genTerrainBlocks(World world, Random rng, Block[] blocks, byte[] bytes, int x, int z, double elevation)
    {
    	/*
        double d1 = plantNoise.func_151601_a((double)x * 0.25D, (double)z * 0.25D);

        if (d1 > 0.0D)
        {
        */
            int k = x & 15;
            int l = z & 15;
            int i1 = blocks.length / 256;

            for (int j1 = 255; j1 >= 0; --j1)
            {
                int k1 = (l * 16 + k) * i1 + j1;

                if (blocks[k1] == null || blocks[k1].getMaterial() != Material.air)
                {
                    if (j1 == 62 && blocks[k1] != Blocks.water)
                    {
                        blocks[k1] = Blocks.water;

                        /*if (d1 < 0.12D)
                        {
                            blocks[k1 + 1] = Blocks.waterlily;
                        }*/
                    }

                    break;
                }
            }
            /*
        }
*/
        this.genBiomeTerrain(world, rng, blocks, bytes, x, z, elevation);
    }

}