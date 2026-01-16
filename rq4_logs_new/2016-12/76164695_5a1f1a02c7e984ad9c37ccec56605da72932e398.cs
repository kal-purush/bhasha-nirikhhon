using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class PreTouch : MonoBehaviour {

	public Image fullScreen;
	public RawImage cursor;

	Transform trans;

	float screenWidth, screenHeight, ratio_x, ratio_y, size;
	const float PHONE_HEIGHT = 11.3f;
	const float PHONE_WIDTH = 6.4f;
	const float PHONE_DELTA = 5.0f;
	const float TO_SCREEN_DIST = 2.0f;
	const float AWAY_SCREEN_DIST = 8.0f;

	// Use this for initialization
	void Start() {
		screenWidth = fullScreen.rectTransform.rect.width;
		screenHeight = fullScreen.rectTransform.rect.height;
	}
	
	public void Move(Vector3 dot) {
		dot.x -= PHONE_DELTA;
		ratio_y = -(dot.x / PHONE_HEIGHT - 0.5f);
		ratio_x = dot.y / PHONE_WIDTH - 0.5f;
		if (dot.z < 0 || dot.z >= AWAY_SCREEN_DIST) 
			size = 0;
		else if (dot.z <= TO_SCREEN_DIST) 
			size = 1;
		else
			size = (AWAY_SCREEN_DIST - dot.z) / (AWAY_SCREEN_DIST - TO_SCREEN_DIST);
	}

	void Update() {
		cursor.transform.localPosition = new Vector3(ratio_x * screenWidth, ratio_y * screenHeight, 0f);
		cursor.transform.localScale = new Vector3(size, size, size);
	}
}