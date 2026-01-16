using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShipNavigator : MonoBehaviour {
	private static List<Obstacle> Obstacles = new List<Obstacle>();
	const float WaypointRadius = 0.1f;

	// Ship acceleration.
	public float Acceleration;

	// Turning acceleration.
	public float SteerAcceleration;

	// Time between navigation updates.
	public float ReactionTime;

	// Safety margin, multiplied by the obstacle radius.
	public float SafetyMargin;

	// This ship's rigid body.
	private Rigidbody2D Body;

	// Our final destination, the island.
	private Vector2 Target;

	// The next time at which we navigate.
	private float NextReaction;

	// The angle we want to point towards.
	private float TargetAngle;

	// Width of the ship.
	private float ShipWidth;


	void Start() {
		this.Body = this.GetComponent<Rigidbody2D>();
		this.Target = Vector2.zero;
		this.NextReaction = Time.time;
		this.ShipWidth = this.GetComponent<CapsuleCollider2D>().size.x * this.transform.localScale.x;
		Debug.LogFormat("Ship width: {0}", this.ShipWidth);
	}
	
	void Update() {
		if (Time.time >= this.NextReaction) {
			this.Navigate();
		}

		float angle = Mathf.Deg2Rad * this.transform.eulerAngles.z - Mathf.PI * 0.5f;
		float deltaAngle = ClampAngle(angle - TargetAngle);
		float turn;
		if (Mathf.Abs(deltaAngle) < 0.1f) {
			turn = 0.0f;
		} else if (Mathf.Abs(deltaAngle) < 0.5f) {
			turn = 0.5f;
		} else {
			turn = 1.0f;
		}
		this.Body.AddTorque(-this.Body.inertia * turn * Mathf.Sign(deltaAngle) * this.SteerAcceleration);

		float accel = 0.0f;
		if (Mathf.Abs(deltaAngle) < Mathf.PI * 0.4) {
			accel = 1.0f;
		} else if (Mathf.Abs(deltaAngle) < Mathf.PI * 0.6) {
			accel = 0.0f;
		} else {
			accel = -0.5f;
		}
		this.Body.AddRelativeForce(new Vector2(0.0f, -this.Body.mass * this.Acceleration * accel));
	}

	void Navigate() {
		Debug.Log("Navigating");
		Vector2 pos = this.transform.position;
		Vector2 targetDelta = this.Target - pos;
		Vector2 targetDir = targetDelta.normalized;
		float targetDistance = Vector2.Dot(targetDelta, targetDir);
		float targetAngle = Mathf.Atan2(targetDir.y, targetDir.x);

		// Get list of obstacles between here and the target.
		Obstacles.Clear();
		foreach (GameObject obj in GameObject.FindGameObjectsWithTag("Obstacle")) {
			float radius;
			CircleCollider2D coll = obj.GetComponent<CircleCollider2D>();
			if (coll == null) {
				radius = 0.2f;
			} else {
				radius = coll.radius * obj.transform.localScale.x;
			}
			radius += this.ShipWidth;
			radius *= this.SafetyMargin;
			Vector2 objPos = obj.transform.position;
			float distance = Vector2.Dot(objPos - pos, targetDir);
			if (distance < 0.05f || distance > targetDistance - 0.05f) {
				continue;
			}
			Obstacles.Add(new Obstacle {
				Position = obj.transform.position,
				Radius = radius,
				Distance = distance,
			});
		}

		// Sort them by distance.
		Obstacles.Sort(new Closer());
		// Range of angles we can steer, relative to target, without colliding with a rock.
		float minSteer = -Mathf.PI * 0.5f, maxSteer = Mathf.PI * 0.5f;
		foreach (Obstacle obj in Obstacles) {
			Vector2 objDelta = obj.Position - pos;
			float avoidAngle = Mathf.Asin(obj.Radius / objDelta.magnitude);
			if (avoidAngle >= 0.4f * Mathf.PI) {
				continue;
			}
			float objAngle = Mathf.Atan2(objDelta.y, objDelta.x);
			float deltaObjAngle = ClampAngle(objAngle - targetAngle);
			float objSteer;
			if (deltaObjAngle > 0.0f) {
				// Obstacle is to the left, so we steer around the right side of it.
				objSteer = deltaObjAngle - avoidAngle;
				if (objSteer < maxSteer) {
					if (objSteer < minSteer) {
						break;
					}
					maxSteer = objSteer;
					if (maxSteer <= 0.1f) {
						break;
					}
				}
			} else {
				// Obstacle is to the right, so we steer around the left side of it.
				objSteer = deltaObjAngle + avoidAngle;
				if (objSteer > minSteer) {
					if (objSteer > maxSteer) {
						break;
					}
					minSteer = objSteer;
					if (minSteer >= -0.1f) {
						break;
					}
				}
			}
		}
		Debug.LogFormat("Steer range: {0} to {1}", minSteer, maxSteer);
		if (minSteer > 0.0f) {
			this.TargetAngle = ClampAngle(targetAngle + minSteer);
		} else if (maxSteer < 0.0f) {
			this.TargetAngle = ClampAngle(targetAngle + maxSteer);
		} else {
			this.TargetAngle = targetAngle;
		}

		this.NextReaction = Time.time + Random.Range(this.NextReaction * 0.8f, this.NextReaction * 1.2f);
	}

	private static float ClampAngle(float a) {
		float r = a % (2.0f * Mathf.PI);
		if (r > Mathf.PI) {
			r -= 2.0f * Mathf.PI;
		} else if (r < -Mathf.PI) {
			r += 2.0f * Mathf.PI;
		}
		return r;
	}

	private struct Obstacle {
		public Vector2 Position;
		public float Radius;
		public float Distance;
	}

	private class Closer : IComparer<Obstacle> {
		public int Compare(Obstacle x, Obstacle y) {
			if (x.Distance < y.Distance) {
				return -1;
			} else if (x.Distance > y.Distance) {
				return 1;
			} else {
				return 0;
			}
		}
	}
}