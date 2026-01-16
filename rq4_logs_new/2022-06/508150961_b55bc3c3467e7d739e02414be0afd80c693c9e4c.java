package com.drastic.sounded;

import net.minecraft.client.Minecraft;
import org.lwjgl.openal.*;

import java.util.List;

public class SoundConstants {
    public List<String> AVAILABLE_DEVICES;

    public SoundConstants() {
        this.refreshDevices();

        Minecraft.getInstance().getSoundManager().soundEngine.soundExecutor = new SoundExecutor(null, ALC11.alcGetCurrentContext());
        Minecraft.getInstance().getSoundManager().soundEngine.musicExecutor = new SoundExecutor(this.AVAILABLE_DEVICES.get(1), 0);

    }

    public void refreshDevices() {
        ALCCapabilities deviceCaps = ALC.createCapabilities(ALC10.alcGetContextsDevice(ALC10.alcGetCurrentContext()));
        if (deviceCaps.OpenALC11) {
            List<String> devices = ALUtil.getStringList(0, ALC11.ALC_ALL_DEVICES_SPECIFIER);
            if (devices != null) {
                this.AVAILABLE_DEVICES = devices;
            }
        }
    }
}