package com.teammetallurgy.atum.world.biome;

import com.teammetallurgy.atum.blocks.AtumBlocks;

import net.minecraft.world.biome.BiomeDecorator;
import net.minecraft.world.biome.BiomeGenBase;
import net.minecraft.world.biome.BiomeGenBase.Height;

public class AtumBiomeGenBase extends BiomeGenBase {

	public AtumBiomeGenBase(int biomeID) {
		super(biomeID);
		
        super.spawnableMonsterList.clear();
        super.spawnableCreatureList.clear();
        super.spawnableWaterCreatureList.clear();
        super.spawnableCaveCreatureList.clear();
        
        this.setDisableRain();
        
        this.topBlock = AtumBlocks.BLOCK_SAND;
        this.fillerBlock = AtumBlocks.BLOCK_STONE;

        this.setColor(16421912);
        
        this.setTemperatureRainfall(2.0F, 0.0F);
        this.setHeight(new Height(0.1F, 0.2F));
	}

    @Override
    public BiomeDecorator createBiomeDecorator() {
        final BiomeDecorator dec = new BiomeDecoratorAtum();
        // dec.treesPerChunk = 1; // defaults to 0
        dec.deadBushPerChunk = 5;
        dec.reedsPerChunk = 0;
        dec.cactiPerChunk = 0;
        
        return dec;
    }

}