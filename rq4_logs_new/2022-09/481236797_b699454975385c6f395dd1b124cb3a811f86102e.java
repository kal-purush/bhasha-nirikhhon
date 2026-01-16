package huige233.transcend.util;

import net.minecraft.entity.Entity;
import net.minecraft.entity.EntityCreature;
import net.minecraft.entity.EntityLiving;
import net.minecraft.entity.EntityLivingBase;
import net.minecraft.entity.item.EntityArmorStand;
import net.minecraft.entity.monster.EntityMob;
import net.minecraft.entity.passive.EntityAmbientCreature;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.inventory.InventoryEnderChest;
import net.minecraft.item.ItemStack;
import net.minecraft.util.DamageSource;
import net.minecraft.util.EntityDamageSource;
import net.minecraft.util.EnumHand;
import net.minecraft.util.math.AxisAlignedBB;
import net.minecraft.world.World;

import java.util.List;

public class SwordUtil {
    public static void killPlayer(EntityPlayer player, EntityLivingBase source){
        ItemStack stack = source.getHeldItem(EnumHand.MAIN_HAND);
        player.inventory.clearMatchingItems(null,-1,-1,null);
        InventoryEnderChest ec = player.getInventoryEnderChest();
        for(int i = 0;i < ec.getSizeInventory();i++){
            ec.removeStackFromSlot(i);
        }
        player.inventory.dropAllItems();
        DamageSource ds = source == null ? new DamageSource("infinity") :new EntityDamageSource("infinity",source);
        DamageSource ds1 = source == null ? new DamageSource("transcend") :new EntityDamageSource("transcend",source);
        player.getCombatTracker().trackDamage(ds,Float.MIN_VALUE,Float.MAX_VALUE);
        player.getCombatTracker().trackDamage(ds1,Float.MIN_VALUE,Float.MAX_VALUE);
        player.setHealth(0.0f);
        player.onDeath(ds);
        player.isDead=true;
    }
    public static void killEntityLiving(EntityLivingBase entity,EntityLivingBase source){
        if(!(entity.world.isRemote || entity.isDead || entity.getHealth()==0.0f)){
            entity.setEntityInvulnerable(false);
            DamageSource ds = source == null ? new DamageSource("infinity") :new EntityDamageSource("infinity",source);
            DamageSource ds1 = source == null ? new DamageSource("transcend") :new EntityDamageSource("transcend",source);
            entity.getCombatTracker().trackDamage(ds,Float.MAX_VALUE,Float.MAX_VALUE);
            entity.getCombatTracker().trackDamage(ds1,Float.MIN_VALUE,Float.MAX_VALUE);
            entity.setHealth(0.0f);
            entity.isDead=true;
        }
    }
    public static void killEntity(Entity entity){
        entity.setDead();
    }
    public static int killRangeEntity(World world,EntityLivingBase entity){
        int range = 50;
        List<Entity> list = world.getEntitiesWithinAABB(EntityLivingBase.class,new AxisAlignedBB(entity.posX - range, entity.posY - range, entity.posZ - range, entity.posX + range, entity.posY + range, entity.posZ + range));
        list.removeIf(en -> en instanceof EntityPlayer || en instanceof EntityArmorStand || en instanceof EntityAmbientCreature || (en instanceof EntityCreature && !(en instanceof EntityMob)));
        list.remove(entity);
        for(Entity en : list) {
            if(en instanceof EntityPlayer){
                killPlayer((EntityPlayer) en,entity);
            }else if(en instanceof EntityLivingBase){
                killEntityLiving((EntityLivingBase) en,entity);
            } else {
                killEntity((Entity) en);
            }
        }
        return list.size();
    }
}