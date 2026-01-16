package net.drugunMC.compound_origins.mixin;

import net.minecraft.client.MinecraftClient;
import net.minecraft.client.render.BufferBuilderStorage;
import net.minecraft.client.render.WorldRenderer;
import net.minecraft.client.render.block.entity.BlockEntityRenderDispatcher;
import net.minecraft.client.render.entity.EntityRenderDispatcher;
import net.minecraft.client.world.ClientWorld;
import net.minecraft.particle.ParticleTypes;
import net.minecraft.sound.SoundCategory;
import net.minecraft.sound.SoundEvents;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.math.random.Random;
import org.jetbrains.annotations.Nullable;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;


@Mixin(WorldRenderer.class)
public abstract class WorldRendererMixin {


    @Shadow @Nullable private ClientWorld world;

    public WorldRendererMixin(MinecraftClient client, EntityRenderDispatcher entityRenderDispatcher, BlockEntityRenderDispatcher blockEntityRenderDispatcher, BufferBuilderStorage bufferBuilders) {
        super();
    }



    // Added events have IDs starting at 59747840

    @Inject(method = "processWorldEvent", at = @At("TAIL"))
    public void CompoundOriginsProcessWorldEventInject(int eventId, BlockPos pos, int data, CallbackInfo ci){
        Random random2 = this.world.random;
        if(eventId == 59747840){    // poison projectile
            this.world.playSoundAtBlockCenter(pos, SoundEvents.BLOCK_SLIME_BLOCK_BREAK, SoundCategory.PLAYERS, 1.0f, 1.0f, false);
            for(int i = 0; i < 30; i++){
                this.world.addParticle(ParticleTypes.ITEM_SLIME, pos.getX()+random2.nextDouble(), pos.getY()+random2.nextDouble(), pos.getZ()+random2.nextDouble(), 1.0f*(random2.nextDouble()-0.5f), 1.0f*(random2.nextDouble()-0.5f), 1.0f*(random2.nextDouble()-0.5f));
            }
        }
        if(eventId == 59747841){    // teleport
            this.world.playSoundAtBlockCenter(pos, SoundEvents.ENTITY_ENDERMAN_TELEPORT, SoundCategory.PLAYERS, 1.0f, 1.0f, false);
            for(int i = 0; i < 15; i++){
                this.world.addParticle(ParticleTypes.DRAGON_BREATH, pos.getX()+random2.nextDouble(), pos.getY()+(2*random2.nextDouble()), pos.getZ()+random2.nextDouble(), 0.06f*(random2.nextDouble()-0.5f), 0.06f*(random2.nextDouble()-0.5f), 0.06f*(random2.nextDouble()-0.5f));
            }
        }
        if(eventId == 59747842){    // freezing projectile
            this.world.playSoundAtBlockCenter(pos, SoundEvents.ENTITY_PLAYER_SPLASH_HIGH_SPEED, SoundCategory.PLAYERS, 1.0f, 1.0f, false);
            for(int i = 0; i < 30; i++){
                this.world.addParticle(ParticleTypes.SNOWFLAKE, pos.getX()+random2.nextDouble(), pos.getY()+random2.nextDouble(), pos.getZ()+random2.nextDouble(), 1.0f*(random2.nextDouble()-0.5f), 1.0f*(random2.nextDouble()-0.5f), 1.0f*(random2.nextDouble()-0.5f));
            }
        }
        if(eventId == 59747843){    // incendiary projectile
            for(int i = 0; i < 30; i++){
                this.world.addParticle(ParticleTypes.FLAME, pos.getX()+random2.nextDouble(), pos.getY()+random2.nextDouble(), pos.getZ()+random2.nextDouble(), 0.5f*(random2.nextDouble()-0.5f), 0.5f*(random2.nextDouble()-0.5f), 0.5f*(random2.nextDouble()-0.5f));
            }
        }
        if(eventId == 59747844){    // lava projectile
            this.world.playSoundAtBlockCenter(pos, SoundEvents.BLOCK_LAVA_EXTINGUISH, SoundCategory.PLAYERS, 1.0f, 1.0f, false);
            for(int i = 0; i < 30; i++){
                this.world.addParticle(ParticleTypes.FLAME, pos.getX()+random2.nextDouble(), pos.getY()+random2.nextDouble(), pos.getZ()+random2.nextDouble(), 0.5f*(random2.nextDouble()-0.5f), 0.5f*(random2.nextDouble()-0.5f), 0.5f*(random2.nextDouble()-0.5f));
            }
        }
        if(eventId == 59747845){    // rock projectile
            this.world.playSoundAtBlockCenter(pos, SoundEvents.BLOCK_STONE_BREAK, SoundCategory.PLAYERS, 1.0f, 1.0f, false);
            for(int i = 0; i < 7; i++){
                this.world.addParticle(ParticleTypes.SMOKE, pos.getX()+random2.nextDouble(), pos.getY()+random2.nextDouble(), pos.getZ()+random2.nextDouble(), 0.3f*(random2.nextDouble()-0.5f), 0.3f*(random2.nextDouble()-0.5f), 0.3f*(random2.nextDouble()-0.5f));
            }
        }
    }









}