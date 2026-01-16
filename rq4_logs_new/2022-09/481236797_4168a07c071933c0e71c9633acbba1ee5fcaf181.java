package huige233.transcend.compat.tinkers;

import c4.conarm.lib.traits.AbstractArmorTrait;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.item.ItemStack;
import net.minecraft.util.DamageSource;
import net.minecraft.util.text.TextFormatting;
import net.minecraftforge.event.entity.living.LivingDamageEvent;
import net.minecraftforge.event.entity.living.LivingHurtEvent;

public class TraitArmorFlawless extends AbstractArmorTrait {
    public TraitArmorFlawless() {
        super("flawless",TextFormatting.DARK_GRAY);
    }

    public float onHurt(ItemStack armor,EntityPlayer player, DamageSource source, float damage, float newDamage, LivingHurtEvent evt){
        return 0.0f;
    }

    public float onDamaged(ItemStack armor, EntityPlayer player, DamageSource source, float damage, float newDamage, LivingDamageEvent evt) {
        return 0.0f;
    }
}