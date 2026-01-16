using UnityEngine;
using System.Collections;


public class Player : MonoBehaviour {

	private bool isInArea = true;
	public Rigidbody2D r2d;
	public float speed = 1.0f;
	private float angle;
	private float angleDirection = 1.0f;
	private float angleSpeed = 3.0f;
	private float maxAngle = 30.0f;

	// Use this for initialization
	void Start () {
		r2d = GetComponent<Rigidbody2D> ();
	}
	
	// Update is called once per frame
	void Update () {
		float originalAngle = angle;

		if (-maxAngle < angle && angle < maxAngle) {
			angle += angleDirection * 0.8f;
		} else {
			angle = (maxAngle - angleSpeed) * angleDirection;
			angleDirection *= -1.0f;
		}

		float rad = Mathf.Deg2Rad * angle;
		if (Input.GetMouseButton (0)) {
			r2d.velocity = new Vector2(-Mathf.Sin(rad) * 5.0f, speed);
			angle = originalAngle;
		} else {
			r2d.velocity = new Vector2(0, -speed * 0.5f);
		}

		transform.localRotation = Quaternion.Euler (0.0f, 0.0f, angle);
//		Vector2 direction = new Vector2 (-originalVx, 3.0f);
//		Vector2 vector = new Vector2 (originalVx, 0);
//
//		if (isInArea) {
//
//			vector = new Vector2(originalVx, range(originalVy - speed * 0.1f, -speed * 0.1f, -speed));
//		}
//		// Long press
//		 if (Input.GetMouseButton (0)) {
//			float posX = r2d.position.x;
//			float aimX = 0.0f;
//			if (-2.0f <= posX && posX < 2.0f) { 
//				aimX = Random.Range(-0.1f, 0.1f);
//			}
//			// 移動する向きを求める
//			direction = new Vector2 (aimX, 1.0f).normalized;
//			// 移動
//			vector = direction * speed;
//		}
//		r2d.velocity = vector;
//
//		float targetAngle = Mathf.Atan2 (r2d.velocity.x * 10.0f, 2.0f) * Mathf.Rad2Deg;
//		angle = Mathf.Lerp (angle, targetAngle, Time.deltaTime * 10.0f);
//		transform.localRotation = Quaternion.Euler (0.0f, 0.0f, targetAngle);

	}

	void OnTriggerEnter2D(Collider2D c){
		isInArea = true;
	}

	float range(float x, float max, float min) {
		if (max > x) {
			return max;
		}
		if (min < x) {
			return min;
		}
		return x;
	}


	void OnTriggerExit2D(Collider2D c){
		isInArea = false;
	}
}