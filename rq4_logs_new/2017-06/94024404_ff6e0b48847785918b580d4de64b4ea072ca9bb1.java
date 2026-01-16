package com.teambr.mako.api.mako;

import net.minecraft.util.ResourceLocation;

public class CombinedMako implements IMako {

    private String name;
    private ResourceLocation icon;
    private int color;
    private IMako parentFirst;
    private IMako parentSecond;

    public CombinedMako(String name, ResourceLocation icon, int color, IMako parentFirst, IMako parentSecond) {
        this.name = name;
        this.icon = icon;
        this.color = color;
        this.parentFirst = parentFirst;
        this.parentSecond = parentSecond;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ResourceLocation getIcon() {
        return icon;
    }

    @Override
    public int color() {
        return 0;
    }
}