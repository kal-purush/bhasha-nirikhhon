package com.trackingzombie.legacy.CustomTemplates;

import java.util.Random;
import net.minecraft.block.Block;
import net.minecraft.util.BlockPos;
import net.minecraft.world.World;
import net.minecraft.world.chunk.IChunkProvider;
import net.minecraftforge.fml.common.IWorldGenerator;

public class CustomBlockGen implements IWorldGenerator
{
	private CustomBlocks genblock;
	private int GenMinVienSize;
	private int GenMaxVienSize;
	private int GenChance;
	private int GenMinY;
	private int GenMaxY;
	private Block[] genIn;
	private Block[] genNear;
	private int genDistance;
	private boolean genOverworld;
	private boolean genNether;
	private boolean genEnder;
	
	
	public CustomBlockGen(CustomBlocks block, int minVienSize, int maxVienSize, int genChance, int minGenY, int maxGenY, Block[] generateIn, Block[] generateNear, int generateDistance, boolean generateInOverworld, boolean generateInNether, boolean generateInEnder)
	{
		this.genblock = block;
		this.GenMinVienSize = minVienSize;
		this.GenMaxVienSize = maxVienSize;
		this.GenChance = genChance;
		this.GenMinY = minGenY;
		this.GenMaxY = maxGenY;
		this.genIn = generateIn;
		this.genNear = generateNear;
		this.genDistance = generateDistance;
		this.genOverworld = generateInOverworld;
		this.genNether = generateInNether;
		this.genEnder = generateInEnder;
	}
	
	public CustomBlockGen(CustomBlocks block, int minVienSize, int maxVienSize, int genChance, int minGenY, int maxGenY, Block[] generateIn, boolean generateInOverworld, boolean generateInNether, boolean generateInEnder)
	{
		this(block, minVienSize, maxVienSize, genChance, minGenY, maxGenY, generateIn, null, 1, generateInOverworld, generateInNether, generateInEnder);
	}
	
	public void generateOre(Block block, World world, Random random, int chunkX, int chunkZ, int minVienSize, int maxVienSize, int chance , int minY, int maxY, Block[] generateIn)
	{
		int vienSize = minVienSize + random.nextInt(maxVienSize - minVienSize);
		int hightRange = maxY - minY;
		
		WorldGenMineable gen = new WorldGenMineable(block.getDefaultState(), vienSize, generateIn, this.genNear, this.genDistance);
		for(int i = 0; i < chance; i++)
		{
			int xRand = chunkX * 16 + random.nextInt(16);
			int yRand = random.nextInt(hightRange) + minY;
			int zRand = chunkZ * 16 + random.nextInt(16);
			gen.generate(world, random, new BlockPos(xRand, yRand, zRand));
		}
	}
	
	public void generate(Random random, int chunkX, int chunkZ, World world, IChunkProvider chunkGenerator, IChunkProvider chunkProvider) 
	{	     
		int currentDimension = world.provider.getDimensionId();
		
		if (currentDimension == -1 && genNether)
		{
			generateOre((Block)this.genblock, world, random, chunkX, chunkZ, this.GenMinVienSize, this.GenMaxVienSize, this.GenChance, this.GenMinY, this.GenMaxY, this.genIn);
		}
		else if(currentDimension == 0 && genOverworld)
		{
			generateOre((Block)this.genblock, world, random, chunkX, chunkZ, this.GenMinVienSize, this.GenMaxVienSize, this.GenChance, this.GenMinY, this.GenMaxY, this.genIn);
		}
		else if(currentDimension == 1 && genEnder)
		{
			generateOre((Block)this.genblock, world, random, chunkX, chunkZ, this.GenMinVienSize, this.GenMaxVienSize, this.GenChance, this.GenMinY, this.GenMaxY, this.genIn);
		}		
	}
}