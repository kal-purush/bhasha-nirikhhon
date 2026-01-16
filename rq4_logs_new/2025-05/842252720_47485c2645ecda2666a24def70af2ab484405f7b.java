package me.nabdev.cosmictooltips.mixins;

import com.badlogic.gdx.Gdx;
import finalforeach.cosmicreach.gamestates.GameState;
import finalforeach.cosmicreach.ui.UI;
import me.nabdev.cosmictooltips.utils.TooltipUtils;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(GameState.class)
public class GameStateMixin {
    @Inject(method = "render", at = @At("HEAD"))
    private void render(CallbackInfo ci) {
        if(TooltipUtils.curTooltip != null && (TooltipUtils.curTooltip.shouldBeHidden() || !UI.renderUI || Gdx.input.isCursorCatched())) {
            TooltipUtils.curTooltip.remove();
            TooltipUtils.curTooltip = null;
        }
    }
}