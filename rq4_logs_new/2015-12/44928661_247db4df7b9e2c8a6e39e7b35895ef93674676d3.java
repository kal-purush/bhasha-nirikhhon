package cz.trigon.bicepsrendererapi.obj;

import java.io.IOException;

import cz.trigon.bicepsrendererapi.game.Surface;
import cz.trigon.bicepsrendererapi.managers.SoundManager;
import cz.trigon.bicepsrendererapi.managers.content.ContentManager;
import cz.trigon.bicepsrendererapi.managers.interfaces.ILoadable;

public class SoundEffect implements ILoadable {
    private static Surface surface;

    public static void init(Surface s) {
        SoundEffect.surface = s;
    }

    private int soundId, lastPlayId;
    private float volume = 1f;
    private SoundManager sound;

    public SoundEffect() {
        this.sound = SoundEffect.surface.getSound();
    }

    @Override
    public void load(ContentManager content, String path) throws IOException {
        this.soundId = this.sound.addSound(content.getDescriptor(path));
    }

    @Override
    public boolean canLoad(ContentManager content, String path) {
        // TODO
        return true;
    }

    public void setVolume(float volume) {
        this.volume = volume;
    }

    public int play(float volume) {
        return this.lastPlayId = this.sound.playSound(this.soundId, volume);
    }

    public int play() {
        return this.play(this.volume);
    }

    public void stopLast() {
        this.stop(this.lastPlayId);
    }

    public void stop(int id) {
        this.sound.stopSound(this.soundId);
    }
}