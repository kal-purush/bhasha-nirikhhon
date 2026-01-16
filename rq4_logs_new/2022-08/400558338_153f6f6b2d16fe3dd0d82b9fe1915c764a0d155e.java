package com.github.salvadormg15.rubber_duck.client.render.curio;

import com.github.salvadormg15.rubber_duck.common.core.Registries;
import top.theillusivec4.curios.api.client.CuriosRendererRegistry;

public class CurioRenderers {
    public static void register() {
        CuriosRendererRegistry.register(Registries.RUBBER_DUCK_ITEM.get(), () -> new RubberDuckCurioRenderer());
    }
}