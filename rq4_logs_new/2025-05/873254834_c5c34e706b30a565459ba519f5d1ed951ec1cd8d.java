package com.github.minecraftschurlimods.bibliowoods.mods;

import com.github.minecraftschurlimods.bibliocraft.api.woodtype.RegisterBibliocraftWoodTypesEvent;
import com.github.minecraftschurlimods.bibliowoods.AbstractMod;
import com.lance5057.butchercraft.ButchercraftBlocks;
import net.minecraft.world.level.block.state.properties.WoodType;
import net.neoforged.bus.api.IEventBus;

import java.lang.invoke.MethodHandles;

public class ButchercraftMod extends AbstractMod {
	private static final WoodType BARN_WOOD_TYPE;
	static {
		try {
			BARN_WOOD_TYPE = (WoodType) MethodHandles.privateLookupIn(ButchercraftBlocks.class, MethodHandles.lookup()).findStaticGetter(ButchercraftBlocks.class, "BARN_WOOD_TYPE", WoodType.class).invoke();
		} catch (Throwable e) {
			throw new RuntimeException("Failed to find and invoke handle for ButchercraftBlocks.BARN_WOOD_TYPE", e);
		}
	}

	public ButchercraftMod(String modId, IEventBus bus) {
		super(modId, bus);
	}

	@Override
	protected void registerWoodTypes(RegisterBibliocraftWoodTypesEvent event) {
		woodType(event, "barn_wood", "block/barn_wood_block", BARN_WOOD_TYPE, ButchercraftBlocks.BARN_WOOD_BLOCK, ButchercraftBlocks.BARN_WOOD_SLAB, ButchercraftBlocks.BARN_WOOD_STAIRS);
	}
}