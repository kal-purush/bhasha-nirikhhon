using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BodyEntityBehaviour : MonoBehaviour {

    public BodyGenerator body;

    public int vertice1 = 0;
    public int vertice2 = 1;

    public float pos = 0.5f;
    public float relativeAngle = -90f;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        Vector3 v1 = body.vertices[vertice1];
        Vector3 v2 = body.vertices[vertice2];

        float angle = Vector3.Angle(v1, v2) + relativeAngle;
        transform.rotation = Quaternion.Euler(0, 0, -angle);
        transform.position = Vector3.Lerp(v1, v2, pos) + body.transform.position;
	}
}