using UnityEngine;
using System.Collections;

public class EndlessStars : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {

        MeshRenderer mr = GetComponent<MeshRenderer>();

        Material mat = mr.material;

        Vector2 offset = mat.mainTextureOffset / 100;

        //offset.x = transform.position.x / transform.localScale.x;
        //offset.y = transform.position.y / transform.localScale.y;

        offset.x += Time.deltaTime;

        mat.mainTextureOffset = offset;
	
	}
}