package com.tassaron.copperlamp.mixin;

import com.tassaron.copperlamp.block.CopperTorchBlock;
import com.tassaron.copperlamp.block.WallCopperTorchBlock;
import net.minecraft.block.Block;
import net.minecraft.block.BlockState;
import net.minecraft.entity.LightningEntity;
import net.minecraft.server.world.ServerWorld;
import net.minecraft.util.math.BlockPos;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import static com.tassaron.copperlamp.CopperLampMod.*;

@Mixin(LightningEntity.class)
public class LightningEntityMixin {
    @Inject(at = @At(value = "INVOKE", target = "Lnet/minecraft/entity/LightningEntity;cleanOxidation(Lnet/minecraft/world/World;Lnet/minecraft/util/math/BlockPos;)V"), method = "tick()V")
    private void tick(CallbackInfo info) {
        LOGGER.info("Copper Lamp checking for copper on a stick...");
        LightningEntity that = ((LightningEntity)(Object)this);
        BlockPos pos = that.getBlockPos();
        ServerWorld world = (ServerWorld) that.getWorld();
        BlockState blockState = world.getBlockState(pos);
        Block block = blockState.getBlock();
        if (block instanceof CopperTorchBlock) {
            world.setBlockState(pos, LIT_COPPER_TORCH.getDefaultState(), 2);
        }
        if (block instanceof WallCopperTorchBlock) {
            world.setBlockState(pos, LIT_COPPER_WALL_TORCH.getDefaultState().with(WallCopperTorchBlock.FACING, blockState.get(WallCopperTorchBlock.FACING)), 2);
        }
    }
}