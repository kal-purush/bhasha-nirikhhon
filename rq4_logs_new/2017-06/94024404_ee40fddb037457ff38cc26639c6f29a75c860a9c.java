package com.teambr.mako.proxy.event;

import com.teambr.mako.proxy.CommonProxy;
import net.minecraft.block.Block;
import net.minecraft.item.Item;
import net.minecraftforge.event.RegistryEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;

@Mod.EventBusSubscriber
public class MakoRegistryEvent {

    @SubscribeEvent
    public static void onRegistryEventBlock(RegistryEvent.Register<Block> event) {
        CommonProxy.blockRegistry.forEach((s, iRegistrable) -> iRegistrable.register(event.getRegistry()));
    }

    @SubscribeEvent
    public static void onRegistryEventItem(RegistryEvent.Register<Item> event) {
        CommonProxy.blockRegistry.forEach((s, iRegistrable) -> iRegistrable.registerItem(event.getRegistry()));
    }
}