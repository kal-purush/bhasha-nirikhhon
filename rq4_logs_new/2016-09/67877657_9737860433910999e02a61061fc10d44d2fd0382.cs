using UnityEngine;
using System.Collections;

public class Movement : MonoBehaviour {
    float scale;
    float offsetX;
    float offsetY;
    float timeScale;
	// Use this for initialization
	void Start () {
        scale = 0.001f;
        offsetX = Random.value;
        offsetY = Random.value;
        //timeScale
        
	}
	
	public void Move()
    {
        transform.Translate(transform.forward * Mathf.Sin(timeScale * (Time.time + offsetX * 2 * Mathf.PI)) * scale);
        transform.Translate(transform.right * Mathf.Cos(timeScale * (Time.time + offsetY * 2 * Mathf.PI)) * scale);

    }
}