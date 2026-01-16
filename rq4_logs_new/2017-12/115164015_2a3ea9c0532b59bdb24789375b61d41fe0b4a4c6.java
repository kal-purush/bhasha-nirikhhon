package entities;

import org.lwjgl.util.vector.Vector3f;

/**
 * Created by AllTheMegahertz on 8/31/2017.
 */

public class Location {

	private World world;
	private Vector3f position;
	private float pitch;
	private float yaw;

	public Location(World world, Vector3f position) {
		this.world = world;
		this.position = position;
	}

	public Location(World world, float x, float y, float z) {
		this.position = new Vector3f(x, y, z);
	}

	public Location(World world, Vector3f position, float pitch, float yaw) {
		this.world = world;
		this.position = position;
		this.pitch = pitch;
		this.yaw = yaw;
	}

	public Location(World world, float x, float y, float z, float pitch, float yaw) {
		this.world = world;
		this.position = new Vector3f(x, y, z);
		this.pitch = pitch;
		this.yaw = yaw;
	}

	public World getWorld() {
		return world;
	}

	public float getX() {
		return  position.x;
	}

	public float getY() {
		return position.y;
	}

	public float getZ() {
		return position.z;
	}

	public float getPitch() {
		return pitch;
	}

	public float getYaw() {
		return yaw;
	}

}