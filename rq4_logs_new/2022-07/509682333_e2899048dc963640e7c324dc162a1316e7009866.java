package com.zin.medieval_decorations;

import com.zin.medieval_decorations.config.CommonConfig;
import com.zin.medieval_decorations.core.init.BlockEntityInit;
import com.zin.medieval_decorations.core.init.BlockInit;
import com.zin.medieval_decorations.core.init.EntityInit;
import com.zin.medieval_decorations.core.init.ItemInit;
import com.zin.medieval_decorations.core.init.SoundInit;

import net.minecraft.world.item.CreativeModeTab;
import net.minecraft.world.item.ItemStack;
import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.api.distmarker.OnlyIn;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.eventbus.api.IEventBus;
import net.minecraftforge.fml.ModLoadingContext;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.config.ModConfig.Type;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;

@Mod("medieval_decorations")
public class MedievalDecorations {

	public static final String MOD_ID = "medieval_decorations";
	
	public static final CreativeModeTab MEDIEVAL_DECORATIONS_TAB = new CreativeModeTab(MOD_ID) {
		@Override
		@OnlyIn(Dist.CLIENT)
		public ItemStack makeIcon() {
			return new ItemStack(ItemInit.MEDIEVAL_DECORATIONS_LOGO.get());
		}

	};

	public MedievalDecorations() {
		IEventBus bus = FMLJavaModLoadingContext.get().getModEventBus();
		
		BlockInit.BLOCKS.register(bus);
		BlockEntityInit.BLOCK_ENTITIES.register(bus);
		EntityInit.ENTITIES.register(bus);
		ItemInit.ITEMS.register(bus);
		SoundInit.SOUNDS.register(bus);
		
        ModLoadingContext.get().registerConfig
        (Type.COMMON, CommonConfig.COMMON_SPEC, MedievalDecorations.MOD_ID + "-common.toml");

        
		MinecraftForge.EVENT_BUS.register(this);

	}

}