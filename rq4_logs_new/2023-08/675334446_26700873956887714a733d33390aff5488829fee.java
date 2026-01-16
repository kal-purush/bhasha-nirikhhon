package dev.statesecurity.smartnametags;

import net.fabricmc.api.ModInitializer;
import net.minecraft.block.Block;
import net.minecraft.block.Blocks;
import net.minecraft.registry.Registries;
import net.minecraft.registry.Registry;
import net.minecraft.util.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartNameTags implements ModInitializer {

    public static final Block CLIENT_SIDE_CHECK = new Block(Block.Settings.copy(Blocks.STONE));
    public static final Logger LOGGER = LoggerFactory.getLogger("smartnametags");

    @Override
    public void onInitialize() {
        LOGGER.info("[SmartNameTags] V1 Started!");
        // Register your custom block to make sure the mod is installed on the client side
        Registry.register(Registries.BLOCK, new Identifier("smartnametags", "smartnametagschecker"), CLIENT_SIDE_CHECK);
    }
}
