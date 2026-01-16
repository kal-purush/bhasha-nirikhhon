package huige233.transcend.effect;

import huige233.transcend.Main;
import huige233.transcend.init.ModItems;
import huige233.transcend.init.TranscendPotions;
import huige233.transcend.packet.PacketEndTimeStop;
import huige233.transcend.util.EntityUtils;
import huige233.transcend.util.ISyncedPotion;
import huige233.transcend.util.Reference;
import huige233.transcend.util.handlers.TranscendPacketHandler;
import net.minecraft.entity.Entity;
import net.minecraft.entity.EntityLivingBase;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.entity.projectile.EntityArrow;
import net.minecraft.util.ResourceLocation;
import net.minecraft.world.World;
import net.minecraftforge.event.entity.living.LivingEvent.LivingUpdateEvent;
import net.minecraftforge.event.entity.living.PotionEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import net.minecraftforge.fml.common.gameevent.PlayerEvent;
import net.minecraftforge.fml.common.gameevent.TickEvent;

import java.util.ArrayList;
import java.util.List;
@Mod.EventBusSubscriber
public class TimeStopEffect extends effectbase implements ISyncedPotion {

    public static final String KEY = "time_stop";
    public TimeStopEffect(boolean isBadEffect,int liquidColour) {
        super(isBadEffect, liquidColour, new ResourceLocation(Reference.MOD_ID, "textures/gui/potion_time_stop.png"));
        this.setPotionName("potion." + Reference.MOD_ID + ":time_stop");
    }

    public static void unblockByEntities(EntityLivingBase host){
        List<Entity> target = EntityUtils.getEntitiesWithinRadius(50,host.posX,host.posY,host.posZ, host.world,Entity.class);
        target.forEach(e -> e.updateBlocked = false);
    }

    private static void performEffectConsistent(EntityLivingBase host,int strength){
        boolean time_stop =  host instanceof EntityPlayer&& host.getHeldItemMainhand().getItem() == ModItems.TRANSCEND_SWORD;
        int interval = strength *4 + 6;
        List<Entity> target = EntityUtils.getEntitiesWithinRadius(50,host.posX,host.posY,host.posZ,host.world,Entity.class);
        target.remove(host);
        target.removeIf(t -> t instanceof EntityLivingBase && ((EntityLivingBase)t).isPotionActive(TranscendPotions.time_stop));
        target.removeIf(t -> t instanceof EntityArrow && t.isEntityInsideOpaqueBlock());

        for(Entity entity : target) {
            entity.getEntityData().setBoolean(KEY,true);
            entity.updateBlocked = time_stop || host.ticksExisted % interval != 0;

            if (!time_stop && entity.world.isRemote) {
                if (entity.onGround) entity.motionY = 0;
            }
            if (entity.updateBlocked) {
                double x = entity.posX + entity.motionX * 1d / (double) interval;
                double y = entity.posY + entity.motionY * 1d / (double) interval;
                double z = entity.posZ + entity.motionZ * 1d / (double) interval;

                entity.prevPosX = entity.posX;
                entity.prevPosY = entity.posY;
                entity.prevPosZ = entity.posZ;

                entity.posX = x;
                entity.posY = y;
                entity.posZ = z;
            } else {
                entity.posX += entity.motionX * 1d / (double) interval;
                entity.posY += entity.motionY * 1d / (double) interval;
                entity.posZ += entity.motionZ * 1d / (double) interval;

                double x = entity.posX - entity.motionX * 1d / (double) interval;
                double y = entity.posY - entity.motionY * 1d / (double) interval;
                double z = entity.posZ - entity.motionZ * 1d / (double) interval;

                entity.prevPosX = x;
                entity.prevPosY = y;
                entity.prevPosZ = z;
            }
        }
        List<Entity> list = EntityUtils.getEntitiesWithinRadius(60,host.posX,host.posY,host.posZ,host.world,Entity.class);
        list.removeAll(target);
        list.forEach(e -> e.updateBlocked = false);
    }

    public static void cleanUpEntities(World world){
        List<Entity> loadedEntity = new ArrayList<>(world.loadedEntityList);
        for(Entity entity : loadedEntity){
            if(entity.getEntityData().getBoolean(KEY)){
                List<EntityLivingBase> nearby = EntityUtils.getLivingWithinRadius(50,entity.posX,entity.posY,entity.posZ,entity.world);
                if(nearby.stream().noneMatch(e -> e.isPotionActive(TranscendPotions.time_stop))){
                    entity.getEntityData().removeTag(KEY);
                    entity.updateBlocked = false;
                }
            }
        }
    }

    @SubscribeEvent
    public static void onLivingUpdateEvent(LivingUpdateEvent event){

        EntityLivingBase entity = event.getEntityLiving();
        if(entity.isPotionActive(TranscendPotions.time_stop)){
            performEffectConsistent(entity, entity.getActivePotionEffect(TranscendPotions.time_stop).getAmplifier());
        }
    }


    @SubscribeEvent
    public static void onPotionAddedEvent(PotionEvent.PotionAddedEvent event){
        if(event.getEntity().world.isRemote && event.getPotionEffect().getPotion() == TranscendPotions.time_stop
                && event.getEntity() instanceof EntityPlayer){
            Main.proxy.playBlinkEffect((EntityPlayer) event.getEntity());
        }
    }

    @SubscribeEvent
    public static void tick(TickEvent.WorldTickEvent event){
        if(!event.world.isRemote && event.phase == TickEvent.Phase.END) cleanUpEntities(event.world);
    }

    @SubscribeEvent
    public static void onPlayerLoggedOutEvent(PlayerEvent.PlayerLoggedOutEvent event){
        if(event.player.updateBlocked) event.player.updateBlocked = false;
    }

    /*
    @SubscribeEvent
    public static void onPotionExpiryEvent(PotionEvent.PotionExpiryEvent event){
        if(event.getPotionEffect() != null && event.getPotionEffect().getPotion() == TranscendPotions.time_stop){
            unblockByEntities(event.getEntityLiving());
            if(!event.getEntity().world.isRemote){
                TranscendPacketHandler.net.sendToDimension(new PacketEndTimeStop.Message(event.getEntityLiving()),event.getEntity().dimension);
            }
        }
    }

    @SubscribeEvent
    public static void onPotionRemoveEvent(PotionEvent.PotionRemoveEvent event){
        if(event.getPotionEffect() != null && event.getPotionEffect().getPotion() == TranscendPotions.time_stop){
            unblockByEntities(event.getEntityLiving());
            if(!event.getEntity().world.isRemote){
                TranscendPacketHandler.net.sendToDimension(new PacketEndTimeStop.Message(event.getEntityLiving()),event.getEntity().dimension);
            }
        }
    }
     */
}