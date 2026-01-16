package com.teammetallurgy.atum.world.biome;

import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

import net.minecraft.util.WeightedRandom;
import net.minecraft.world.gen.NoiseGeneratorSimplex;
import net.minecraft.world.gen.layer.GenLayer;
import net.minecraft.world.gen.layer.IntCache;
import net.minecraftforge.common.BiomeManager.BiomeEntry;

public class GenLayerAtumBiome extends GenLayer {

	private List<BiomeEntry> lBiomes = Lists.newArrayList();
	private List<BiomeEntry> hBiomes = Lists.newArrayList();
	
	private final NoiseGeneratorSimplex noise;
	
	public GenLayerAtumBiome(long seed) {
		super(seed);
		noise = new NoiseGeneratorSimplex(new Random(seed));
		
		for(AtumBiomeGenBase biome : AtumBiomes.biomes ) {
			final BiomeEntry entry = new BiomeEntry(biome, biome.getWeight());
			if( biome.rootHeight >= 0.25F ) {
				hBiomes.add(entry);
			} else {
				lBiomes.add(entry);
			}
		}
	}

	@Override
	public int[] getInts(int x, int z, int width, int length) {
		int[] cache = IntCache.getIntCache(width * length);
		
		int lWeight = WeightedRandom.getTotalWeight(lBiomes);
		int hWeight = WeightedRandom.getTotalWeight(hBiomes);
		
		for( int i = 0; i < length; ++i ) {
			for( int j = 0; j < width; ++j ) {
				this.initChunkSeed((long)(j + x), (long)(i + z));
				final double elevationType = noise.func_151605_a(x,z);
				final BiomeEntry biome;
				if( elevationType <= 0.25 ) {
					biome = ((BiomeEntry)WeightedRandom.getItem(hBiomes, nextInt(hWeight)));
				} else {
					biome = ((BiomeEntry)WeightedRandom.getItem(lBiomes, nextInt(lWeight)));
				}
				cache[j + i * width] = biome.biome.biomeID;
			}
		}
		
		return cache;
	}
}