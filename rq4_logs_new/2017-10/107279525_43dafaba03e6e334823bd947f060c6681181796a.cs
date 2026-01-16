using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerController : MonoBehaviour {

	public int speedtilt;
	public int speedkey;
	public Text countText;
	public Text winText;

	private int count;
	private Rigidbody2D rb2d;

	void Start() {
		rb2d = GetComponent<Rigidbody2D> ();
		count = 0;
		SetCountText ();
		winText.text = "";
	}

	void FixedUpdate () {
		// 加速度
		float moveHorizontal = Input.acceleration.x * speedtilt;
		float moveVertical = Input.acceleration.y * speedtilt;
		// キー
		float axisHorizontal = Input.GetAxis ("Horizontal") * speedkey;
		float axisVertical = Input.GetAxis ("Vertical") * speedkey;
		// キー優先
		if (axisHorizontal != 0 || axisVertical != 0) {
			moveHorizontal = axisHorizontal;
			moveVertical = axisVertical;
		}
		Vector2 movement = new Vector2 (moveHorizontal, moveVertical);
		rb2d.MovePosition (rb2d.position + movement);
	}

	// 衝突
	void OnTriggerEnter2D(Collider2D other) {
		if (other.gameObject.CompareTag ("PickUp")) {
			other.gameObject.SetActive(false);
			count = count + 100;
			SetCountText ();
		}
	}

	void SetCountText() {
		countText.text = "Score: " + count.ToString();
		if (count >= 1200) {
			winText.text = "YOU WIN";
		}
	}

	void damage(int damage) {
		Debug.Log (damage);
	}
}