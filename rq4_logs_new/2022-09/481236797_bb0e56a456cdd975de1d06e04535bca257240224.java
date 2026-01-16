package huige233.transcend.compat.tinkers;

import c4.conarm.common.armor.traits.TraitVoltaic;
import c4.conarm.common.armor.utils.ArmorTagUtil;
import c4.conarm.lib.armor.ArmorNBT;
import c4.conarm.lib.traits.AbstractArmorTrait;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.init.MobEffects;
import net.minecraft.item.ItemStack;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.potion.PotionEffect;
import net.minecraft.util.DamageSource;
import net.minecraft.world.World;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.entity.living.LivingHurtEvent;
import slimeknights.tconstruct.library.modifiers.ModifierNBT;
import slimeknights.tconstruct.library.utils.ModifierTagHolder;
import slimeknights.tconstruct.library.utils.TagUtil;

import java.util.List;

public class TraitArmorTranscend extends AbstractArmorTrait {
    public static final TraitArmorTranscend a = new TraitArmorTranscend();
    public TraitArmorTranscend() {
        super("transcend",0xF8F8FF);
        MinecraftForge.EVENT_BUS.register(this);
    }

    @Override
    public float onHurt(ItemStack armor, EntityPlayer player, DamageSource source, float damage, float newDamage, LivingHurtEvent evt){
        evt.setCanceled(true);
        return 0;
    }

    @Override
    public int onArmorDamage(ItemStack armor, DamageSource source, int damage, int newDamage, EntityPlayer player, int slot) {
        return 0;
    }

    @Override
    public void onArmorTick(ItemStack armor, World world, EntityPlayer player){
        player.setAir(300);
        player.addPotionEffect(new PotionEffect(MobEffects.RESISTANCE, 300, 14, false, false));
        player.addPotionEffect(new PotionEffect(MobEffects.NIGHT_VISION, 300, 0, false, false));
        player.capabilities.allowFlying = true;
        player.addPotionEffect(new PotionEffect(MobEffects.STRENGTH, 300, 14, false, false));
        player.addPotionEffect(new PotionEffect(MobEffects.REGENERATION, 300, 14, false, false));
        List<PotionEffect> effects = Lists.newArrayList(player.getActivePotionEffects());
        for(PotionEffect potion : Collections2.filter(effects, potion -> potion.getPotion().isBadEffect())) {
            player.removePotionEffect(potion.getPotion());
        }
        player.getFoodStats().addStats(20, 20.0f);
        player.addPotionEffect(new PotionEffect(MobEffects.SPEED, 300, 2, false, false));
        player.addPotionEffect(new PotionEffect(MobEffects.JUMP_BOOST, 300, 2, false, false));
        player.stepHeight=3;
        player.setFire(0);
        player.addPotionEffect(new PotionEffect(MobEffects.LUCK, 300, 9, false, false));
        player.addPotionEffect(new PotionEffect(MobEffects.HASTE, 300, 44, false, false));
        if(player.isBurning()) {
            player.extinguish();
        }
    }

    @Override
    public void applyEffect(NBTTagCompound rootCompound, NBTTagCompound modifierTag) {
        super.applyEffect(rootCompound, modifierTag);
        ArmorNBT data = ArmorTagUtil.getArmorStats(rootCompound);
        ArmorNBT orginal = ArmorTagUtil.getOriginalArmorStats(rootCompound);
        data.modifiers += orginal.modifiers + 100;
        TagUtil.setToolTag(rootCompound,data.get());
        rootCompound.setBoolean("Unbreakable", true);
    }
}