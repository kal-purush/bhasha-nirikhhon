package dev.uten2c.raincoat.mixin.entity;

import dev.uten2c.raincoat.States;
import net.minecraft.entity.Entity;
import net.minecraft.entity.EntityType;
import net.minecraft.entity.LivingEntity;
import net.minecraft.world.World;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(LivingEntity.class)
public abstract class MixinLivingEntity extends Entity {
    @Shadow
    public float prevHeadYaw;

    @Shadow
    public float headYaw;

    @Shadow
    public float bodyYaw;

    @Shadow
    public float prevBodyYaw;

    public MixinLivingEntity(EntityType<?> type, World world) {
        super(type, world);
    }

    @Inject(method = "updateTrackedHeadRotation", at = @At("TAIL"))
    private void updatePrevHeadYaw(float yaw, int interpolationSteps, CallbackInfo ci) {
        if (!States.isOnServer()) {
            return;
        }
        prevHeadYaw = yaw;
    }

    @Override
    public void updatePositionAndAngles(double x, double y, double z, float yaw, float pitch) {
        super.updatePositionAndAngles(x, y, z, yaw, pitch);
        if (!States.isOnServer()) {
            return;
        }
        headYaw = yaw;
        prevHeadYaw = yaw;
        bodyYaw = yaw;
        prevBodyYaw = yaw;
    }
}