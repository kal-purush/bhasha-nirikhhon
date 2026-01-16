package com.tassaron.copperlamp;

import net.fabricmc.api.ModInitializer;
import com.tassaron.copperlamp.block.CopperLampBlock;
import net.fabricmc.fabric.api.item.v1.FabricItemSettings;
import net.fabricmc.fabric.api.object.builder.v1.block.FabricBlockSettings;
import net.minecraft.block.*;
import net.minecraft.item.BlockItem;
import net.minecraft.item.ItemGroup;
import net.minecraft.sound.BlockSoundGroup;
import net.minecraft.state.property.Properties;
import net.minecraft.util.Identifier;
import net.minecraft.util.registry.Registry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.function.ToIntFunction;


public class CopperLampMod implements ModInitializer {
	public static final Logger LOGGER = LogManager.getLogger("copperlamp");
	public static final Block COPPER_LAMP = new CopperLampBlock(FabricBlockSettings.of(Material.REDSTONE_LAMP).luminance(createLightLevelFromLitBlockState(15)).strength(0.3F).sounds(BlockSoundGroup.GLASS));

	@Override
	public void onInitialize() {
		Registry.register(Registry.BLOCK, new Identifier("copperlamp", "copper_lamp"), COPPER_LAMP);
		Registry.register(Registry.ITEM, new Identifier("copperlamp", "copper_lamp"), new BlockItem(COPPER_LAMP, new FabricItemSettings().group(ItemGroup.DECORATIONS)));
	}

	private static ToIntFunction<BlockState> createLightLevelFromLitBlockState(int litLevel) {
		return (state) -> {
			return (Boolean)state.get(Properties.LIT) ? litLevel : 0;
		};
	}
}