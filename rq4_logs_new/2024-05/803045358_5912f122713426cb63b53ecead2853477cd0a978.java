package net.elreyjota.revenantmod.item;

import net.elreyjota.revenantmod.RevenantMod;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.chat.Component;
import net.minecraft.world.item.CreativeModeTab;
import net.minecraft.world.item.Item;
import net.minecraft.world.item.ItemStack;
import net.minecraftforge.eventbus.api.IEventBus;
import net.minecraftforge.registries.DeferredRegister;
import net.minecraftforge.registries.RegistryObject;

public class ModCreativeModTabs {
    public static final DeferredRegister<CreativeModeTab> CREATIVE_MODE_TABS =
            DeferredRegister.create(Registries.CREATIVE_MODE_TAB, RevenantMod.MOD_ID);

    public static final RegistryObject<CreativeModeTab> TUTORIAL_TAB = CREATIVE_MODE_TABS.register("revenant_tab",
            () -> CreativeModeTab.builder().icon(() -> new ItemStack(ModItems.LOGO.get()))
                    .title(Component.translatable("creativetab.revenant_tab"))
                    .displayItems((itemDisplayParameters, output) -> {
                        for(RegistryObject<Item>item: ModItems.ITEMS.getEntries()){
                            if(item != ModItems.LOGO) {
                                output.accept(item.get());
                            }
                        }
                    })
                    .build());

    public static void register(IEventBus eventBus){
        CREATIVE_MODE_TABS.register(eventBus);
    }
}