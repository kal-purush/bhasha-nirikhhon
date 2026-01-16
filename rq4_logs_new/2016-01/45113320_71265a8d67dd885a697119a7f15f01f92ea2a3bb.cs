using UnityEngine;
using System.Collections;

public class UFOScript : MonoBehaviour {

	float speed;
	Vector3 direction;
	float r;
	float g;
	float b;
	float rVal;
	float gVal;
	float bVal;
	Color randomCol;

	// Use this for initialization
	void Start () {
		speed = 0.1f;
		r = g = b = 0.01f;
		rVal = 0.1f;
		gVal = 0.05f;
		gVal = 0.025f;
		direction = new Vector3 (speed,0,0);
	}
	
	// Update is called once per frame
	void Update () 
	{
		this.gameObject.transform.position += direction;

		if (r > 0.99f || r < 0.01f){rVal *= -1;}
		if (g > 0.99f || g < 0.01f){gVal *= -1;}
		if (b > 0.99f || b < 0.01f){bVal *= -1;}
		r += rVal;
		g += gVal;
		b += bVal;
		randomCol = new Color(r,g,b);
		this.gameObject.GetComponent<SpriteRenderer> ().color = randomCol;

		if (this.gameObject.transform.position.x > 10){Destroy(this.gameObject);}
	}
}