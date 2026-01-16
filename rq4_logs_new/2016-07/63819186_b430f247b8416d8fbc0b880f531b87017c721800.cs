// notes:
// https://docs.unity3d.com/ScriptReference/Transform.Rotate.html

using UnityEngine;
using System.Collections;

public class Cube : MonoBehaviour {

    public float speed = 20;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        // Rotate the object around its local Y axis at 1 degree per second
        transform.Rotate(Vector3.right * Time.deltaTime * speed);
        // ...also rotate around the World's Y axis
        transform.Rotate(Vector3.up * Time.deltaTime * speed, Space.World);
    }
}