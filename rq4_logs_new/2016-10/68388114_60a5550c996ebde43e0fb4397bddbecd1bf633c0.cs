using UnityEngine;
using System.Collections;

public class MoveCar : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        transform.Translate(Vector2.left * 100 * Time.deltaTime);
    }
}