using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GeesenadoScript : MonoBehaviour {
    private CircleCollider2D geesenadoCirc;
	// Use this for initialization
	void Start () {
        geesenadoCirc = gameObject.AddComponent<CircleCollider2D>();
        System.Random rnd = new System.Random();
        float offsetX = rnd.Next(-80, -22);
        float offsetY = rnd.Next(-90, 65);
        geesenadoCirc.offset = new Vector2(offsetX, offsetY);
        geesenadoCirc.radius = 160.0f;
        //geesenadoCirc.offset = new Vector2(-45f, 3f);
        geesenadoCirc.isTrigger = true;
        /*Debug.Log("STORM IS FORMED");
        Debug.Log("Offset X is:" + offsetX);
        Debug.Log("Offset Y is:" + offsetY);*/
	}
	
	// Update is called once per frame
	void Update () {
        geesenadoCirc.radius = geesenadoCirc.radius - (1.5f * Time.deltaTime);
        //Debug.Log("Storm is shrinking");
	}
}