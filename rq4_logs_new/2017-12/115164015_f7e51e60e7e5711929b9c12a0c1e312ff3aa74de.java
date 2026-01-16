package collision;

import org.lwjgl.util.vector.Vector2f;
import org.lwjgl.util.vector.Vector3f;

/**
 * Created by AllTheMegahertz on 12/22/2017.
 */

public class Aabb {

	private Vector3f center;
	private Vector3f halfExtent;

	public Aabb(Vector3f center, Vector3f halfExtent) {
		this.center = center;
		this.halfExtent = halfExtent;
	}

	public boolean isIntersecting(Aabb box) {

		Vector3f distance = Vector3f.sub(box.getCenter(), center, new Vector3f());
		distance.x = Math.abs(distance.x);
		distance.y = Math.abs(distance.y);
		distance.z = Math.abs(distance.z);

		Vector3f.sub(distance, Vector3f.add(halfExtent, box.getHalfExtent(), new Vector3f()), distance);

		return distance.x < 0 && distance.y < 0 && distance.z < 0;

	}

	public Vector3f getCenter() {
		return center;
	}

	public Vector3f getHalfExtent() {
		return halfExtent;
	}

}