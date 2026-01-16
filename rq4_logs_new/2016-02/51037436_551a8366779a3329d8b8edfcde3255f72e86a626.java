package com.engine.core;

import org.lwjgl.input.Keyboard;
import org.lwjgl.util.vector.Vector3f;

public class Camera {
	
	private Vector3f position;
	private float pitch;
	private float yaw;
	private float roll;
	private float speed;
	
	public Camera() {
		position = new Vector3f(0,0,0);
		speed = 0.02f;
	}
	
	public void input() {
		if(Keyboard.isKeyDown(Keyboard.KEY_W)) {
			position.z -= speed;
		}
		
		if(Keyboard.isKeyDown(Keyboard.KEY_S)) {
			position.z += speed;
		}
		
		if(Keyboard.isKeyDown(Keyboard.KEY_A)) {
			position.x -= speed;
		}
		
		if(Keyboard.isKeyDown(Keyboard.KEY_D)) {
			position.x += speed;
		}
	}
	
	public Vector3f getPosition() {
		return position;
	}
	
	public float getPitch() {
		return pitch;
	}
	
	public float getYaw() {
		return yaw;
	}
	
	public float getRoll() {
		return roll;
	}
	
	public float getSpeed() {
		return speed;
	}
	
	public void setSpeed(float newSpeed) {
		speed = newSpeed;
	}
}