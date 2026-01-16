using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Timer : MonoBehaviour {
	public float temps = 2;

    private Transform indicateur;
	private float vitesse;

    // Use this for initialization
    void Start () {
		indicateur = transform.GetChild(0);
		vitesse = indicateur.gameObject.GetComponent<SpriteRenderer>().bounds.size.x / temps;
	}
	
	// Update is called once per frame
	void Update () {
		if (temps > 0) {
			temps -= Time.deltaTime;
			indicateur.Translate(Time.deltaTime * -vitesse, 0, 0);
		}
	}

	public bool TimeUp()
	{
		return temps <= 0;
	}
}