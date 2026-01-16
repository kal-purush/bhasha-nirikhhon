package com.cookiehook.liquidenchanting;

import java.io.File;

import com.cookiehook.liquidenchanting.init.ModItems;
import com.cookiehook.liquidenchanting.recipes.ModRecipes;
import com.cookiehook.liquidenchanting.util.Config;
import com.cookiehook.liquidenchanting.util.Reference;

import cpw.mods.fml.common.Mod;
import cpw.mods.fml.common.Mod.EventHandler;
import cpw.mods.fml.common.Mod.Instance;
import cpw.mods.fml.common.event.FMLInitializationEvent;
import cpw.mods.fml.common.event.FMLPostInitializationEvent;
import cpw.mods.fml.common.event.FMLPreInitializationEvent;

@Mod(modid = Reference.MOD_ID, version = Reference.VERSION)
public class Main {

	@Instance(Reference.MOD_ID)
	public static Main instance;
	private static File configDir;

	@EventHandler
	public void PreInit(FMLPreInitializationEvent event) {
		configDir = new File(event.getModConfigurationDirectory() + "/" + Reference.MOD_ID);
		configDir.mkdir();
		Config.init(new File(configDir.getPath(), Reference.MOD_ID + ".cfg"));
		
		ModItems.init();
		ModItems.register();
	}

	@EventHandler
	public void init(FMLInitializationEvent event) {
		ModRecipes.registerRecipes();
	}

	@EventHandler
	public void PostInit(FMLPostInitializationEvent event) {

	}
}