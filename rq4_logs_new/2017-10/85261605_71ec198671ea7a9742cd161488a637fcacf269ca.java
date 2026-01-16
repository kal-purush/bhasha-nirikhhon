package de.webtwob.mbma.api;

import de.webtwob.mbma.api.capability.APICapabilities;

import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.event.FMLPreInitializationEvent;

/**
 * Created by BB20101997 on 26. Okt. 2017.
 */

@Mod.EventBusSubscriber
@Mod(modid = MBMA_API.MODID, useMetadata = true)
public class MBMA_API {
    public static final String MODID = "mbmaapi";
    
    @Mod.Instance(MODID)
    public static  MBMA_API INSTANCE;
    
    @Mod.EventHandler
    public void preInit(FMLPreInitializationEvent event) {
        MBMAAPILog.info("Starting PreInit!");
        APICapabilities.register();
        /*
         * other stuff that would go here:
         * Ore-Dict assignment
         */
        MBMAAPILog.info("Finished PreInit!");
    }
}