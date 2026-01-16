package com.solegendary.reignofnether.mixin;

import com.solegendary.reignofnether.unit.interfaces.Unit;
import com.solegendary.reignofnether.util.MiscUtil;
import net.minecraft.core.BlockPos;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.level.BaseSpawner;
import net.minecraft.world.level.Level;
import org.joml.Vector3d;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.List;

@Mixin(BaseSpawner.class)
public class BaseSpawnerMixin {

    @Inject(
            method = "isNearPlayer",
            at = @At("HEAD"),
            cancellable = true
    )
    public void isNearPlayer(Level pLevel, BlockPos pPos, CallbackInfoReturnable<Boolean> cir) {
        List<LivingEntity> activatorEntities = MiscUtil.getEntitiesWithinRange(new Vector3d(pPos.getX(), pPos.getY(), pPos.getZ()), 16, LivingEntity.class, pLevel)
                .stream()
                .filter(e -> (e instanceof Unit unit && !unit.getOwnerName().isBlank()) ||
                             (e instanceof Player player && !player.isSpectator() && !player.isCreative()) &&
                                 e.isAlive()
                ).toList();
        cir.setReturnValue(!activatorEntities.isEmpty());
    }
}