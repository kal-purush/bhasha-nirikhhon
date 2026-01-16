package audio;

import java.io.File;

import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import main.Handler;

public class Audio {

	private String fileName;
	private boolean isAmbient;
	private boolean isAmbientPlaying = false;
	private boolean isAmbientStarted = false;
	private boolean isEffect;
	private boolean completed;
	
	private MediaPlayer mediaPlayer;
	private Media sound;
	
	private Handler handler;
	
	public Audio(String fileName, boolean isAmbient, boolean isEffect, Handler handler) {
		this.fileName = "res/audio/" + fileName;
		this.isAmbient = isAmbient;
		if(this.isAmbient == true)
			this.isAmbientPlaying = true;
		this.isEffect = isEffect;
		this.handler = handler;
		handler.getAudioManager().getSounds().add(this);
	}
	
	public void tick() {
		if(mediaPlayer == null) {
			sound = new Media(new File(fileName).toURI().toString());
			mediaPlayer = new MediaPlayer(sound);
			if(isAmbient) {
				mediaPlayer.setVolume(handler.getAudioManager().getAmbientVolume());
				mediaPlayer.setCycleCount(100000);
			}else if(isEffect) {
				mediaPlayer.setVolume(handler.getAudioManager().getEffectVolume());
				mediaPlayer.setCycleCount(1);
			}
		}
		if(mediaPlayer != null) {
			if(isEffect) {
				mediaPlayer.play();
				if(mediaPlayer.getCurrentTime().toMillis() >= sound.getDuration().toMillis()) {
					handler.getAudioManager().remove(this);
					completed = true;
				}
			}else if(isAmbient) {
				if(!isAmbientPlaying && isAmbientStarted) {
					mediaPlayer.pause();
					return;
				}
				if(!isAmbientStarted) {
					isAmbientStarted = true;
					mediaPlayer.play();
				}
			}
		}
	}
	
	public void stop() {
		mediaPlayer.dispose();
	}
	
	public void stopAmbience() {
		mediaPlayer.pause();
	}
	
	public void startAmbience() {
		mediaPlayer.play();
	}

	public boolean isAmbient() {
		return isAmbient;
	}

	public void setAmbient(boolean isAmbient) {
		this.isAmbient = isAmbient;
	}
	
	public MediaPlayer getMediaPlayer() {
		return mediaPlayer;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}
	
}