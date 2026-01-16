using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DestroyOnCollision : MonoBehaviour {

    public static Color[] hitColors = {
        Color.white,
        Color.white,
        Color.yellow,
        Color.yellow,
        Color.yellow,
        Color.red,
        Color.red,
        Color.red
       
    };

    public string tag = "Bullet";

    private int health;
    private Material mat;

	// Use this for initialization
	void Start () {

        health = hitColors.Length;
        mat = GetComponent<Renderer>().material;
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    void OnTriggerEnter(Collider other) {

        if(other.transform.tag == tag) {

            
            if (--health > 0) {
                mat.color = hitColors[hitColors.Length - health];
            }

            if (health <= 0) {
                Destroy(gameObject);
            }

            Destroy(other.gameObject);

        }

    }

}