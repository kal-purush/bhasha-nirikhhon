package com.github.telvarost.mostlymodernrecipes.events;

import blue.endless.jankson.JsonObject;
import com.github.telvarost.mostlymodernrecipes.ModHelper;
import net.glasslauncher.mods.api.gcapi.api.PreConfigSavedListener;
import net.mine_diver.unsafeevents.listener.EventListener;

@EventListener
public class ConfigListener implements PreConfigSavedListener {

    @Override
    public void onPreConfigSaved(JsonObject jsonObject, JsonObject jsonObject1) {
        /** - Update max stack size on config change */
        ModHelper.AttemptToSetStackSizeOfItems();
    }
}