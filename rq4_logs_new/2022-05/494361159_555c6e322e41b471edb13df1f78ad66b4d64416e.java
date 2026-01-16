package me.patakapata.findcmd.client.mixin;

import me.patakapata.findcmd.client.FindCmdClient;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.RunArgs;
import net.minecraft.client.WindowEventHandler;
import net.minecraft.client.texture.TextureManager;
import net.minecraft.resource.ReloadableResourceManagerImpl;
import net.minecraft.util.profiler.Profiler;
import net.minecraft.util.thread.ReentrantThreadExecutor;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(MinecraftClient.class)
public abstract class MinecraftClientMixin extends ReentrantThreadExecutor<Runnable> implements WindowEventHandler {
    @Shadow
    public abstract TextureManager getTextureManager();

    @Shadow
    @Final
    private TextureManager textureManager;

    @Shadow
    @Final
    private ReloadableResourceManagerImpl resourceManager;

    @Shadow
    private Profiler profiler;

    public MinecraftClientMixin(String string) {
        super(string);
    }

    @Inject(method = "<init>", at = @At(value = "INVOKE", target = "Lnet/minecraft/client/render/entity/model/EntityModelLoader;<init>()V"))
    private void inject_after_textureManagerInit(RunArgs args, CallbackInfo ci) {
        FindCmdClient.registerTexture(this.textureManager, this.resourceManager);
    }
}