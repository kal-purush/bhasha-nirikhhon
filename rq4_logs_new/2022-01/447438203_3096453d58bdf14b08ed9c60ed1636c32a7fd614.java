package com.tassaron.copperlamp;

import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.api.EnvType;
import net.fabricmc.api.Environment;
import net.fabricmc.fabric.api.blockrenderlayer.v1.BlockRenderLayerMap;
import net.minecraft.client.render.RenderLayer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@Environment(EnvType.CLIENT)
public class CopperLampModClient implements ClientModInitializer {
	public static final Logger LOGGER = LogManager.getLogger("copperlamp");

	@Override
	public void onInitializeClient() {
		LOGGER.info("Registering cutout for copper torch");
		BlockRenderLayerMap.INSTANCE.putBlock(CopperLampMod.COPPER_TORCH, RenderLayer.getCutout());
	}
}