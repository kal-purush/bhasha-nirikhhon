package com.ui_utils.mixin;



import com.ui_utils.SharedVariables;


import net.minecraft.item.Item;

import net.minecraft.item.Items;

import net.minecraft.text.Text;


import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(Item.class)
public abstract class ItemMixin {



    @Inject(method = "getName(Lnet/minecraft/item/ItemStack;)Lnet/minecraft/text/Text;", at = @At("HEAD"), cancellable = true)
    public void modifyName(CallbackInfoReturnable<Text> cir) {
        if (SharedVariables.hookersAndCocaine){
            //Check if the item is sugar
            if (this.equals(Items.SUGAR)){
                cir.setReturnValue(Text.of("Cocaine"));
            }
            if (this.equals(Items.FISHING_ROD)){
                cir.setReturnValue(Text.of("Hooker"));
            }





            }

        }




}
