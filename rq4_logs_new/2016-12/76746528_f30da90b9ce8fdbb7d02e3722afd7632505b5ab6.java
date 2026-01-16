package com.Egietje.degeweldigemod;

import org.lwjgl.opengl.Display;

import com.Egietje.degeweldigemod.entities.*;
import com.Egietje.degeweldigemod.handler.*;
import com.Egietje.degeweldigemod.init.*;
import com.Egietje.degeweldigemod.proxy.*;
import com.Egietje.degeweldigemod.world.biome.WorldTypeCheese;
import com.Egietje.degeweldigemod.world.dimension.CheeseTeleporter;
import com.Egietje.degeweldigemod.world.gen.CheeseGeneration;

import net.minecraft.client.Minecraft;
import net.minecraft.client.renderer.entity.RenderManager;
import net.minecraft.entity.EntityLiving;
import net.minecraft.entity.EntitySpawnPlacementRegistry;
import net.minecraft.entity.EnumCreatureType;
import net.minecraft.entity.passive.EntityCow;
import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.init.Biomes;
import net.minecraft.init.MobEffects;
import net.minecraft.potion.PotionEffect;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.WorldType;
import net.minecraft.world.biome.Biome;
import net.minecraft.world.gen.structure.MapGenVillage;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.fml.client.registry.RenderingRegistry;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.Mod.EventHandler;
import net.minecraftforge.fml.common.Mod.Instance;
import net.minecraftforge.fml.common.SidedProxy;
import net.minecraftforge.fml.common.event.FMLInitializationEvent;
import net.minecraftforge.fml.common.event.FMLPostInitializationEvent;
import net.minecraftforge.fml.common.event.FMLPreInitializationEvent;
import net.minecraftforge.fml.common.network.NetworkRegistry;
import net.minecraftforge.fml.common.registry.EntityRegistry;
import net.minecraftforge.fml.common.registry.GameRegistry;

@Mod(modid = Reference.MODID, name = Reference.NAME, version = Reference.VERSION, acceptedMinecraftVersions = "[1.11]")
public class DeGeweldigeMod {
	
	@SidedProxy(clientSide = Reference.CLIENTPROXY, serverSide = Reference.COMMONPROXY)
	public static CommonProxy proxy;
	
	@Instance(Reference.MODID)
	public static DeGeweldigeMod DGMInstance;
	
	@EventHandler
	public void preInit(FMLPreInitializationEvent event) {
		new CheeseBlocks();
		new CheeseItems();
		new CheeseMobs();
		new CheeseAchievements();
		new CheeseSpawnPlacementRegistry();
		new CheeseBiomes();
		new CheeseTabs();
		new CheesePacketHandler();
		new CheeseDimensions();
		proxy.registerModels();
		proxy.renderEntities();
		proxy.setTitle("Minecraft - " + Minecraft.getMinecraft().getVersion() + " | DeGeweldigeMod - " + Reference.VERSION);
	}
	
	@EventHandler
	public static void init(FMLInitializationEvent event) {
		new CheeseCraftingAndSmelting();
		new CheeseCapabilitys();
		proxy.registerTileEntities();
		proxy.registerClientEventHandler();
		proxy.registerCommonEventHandler();
		proxy.registerHandler(DGMInstance);
	}
	
	@EventHandler
	public static void postInit(FMLPostInitializationEvent event) {
		new WorldTypeCheese();
	}
}