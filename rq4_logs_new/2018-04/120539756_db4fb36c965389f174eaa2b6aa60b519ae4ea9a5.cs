using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Goose : MonoBehaviour {
    public GameObject geesenado;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        Vector2 center = geesenado.GetComponent<CircleCollider2D>().offset;
        //transform.LookAt(center);
        //transform.Rotate(new Vector3(0, -90, 0), Space.Self);
        GetComponent<Rigidbody2D>().position = Vector2.MoveTowards(new Vector2(transform.position.x, transform.position.y), center, 3 * Time.deltaTime);
    }
}