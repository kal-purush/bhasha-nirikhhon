using UnityEngine;
using System.Collections;

public class bgTextureScroll : MonoBehaviour {

    public float scrollSpeedX;
    public Renderer rend;
    private float offsetX;
    private float initWorldSpaceX;

    // Use this for initialization
    void Start () {
        offsetX = scrollSpeedX * Time.time;
        rend = GetComponent<Renderer>();
        initWorldSpaceX = transform.position.x;
	}

	// Update is called once per frame
	void Update () {
        offsetX = scrollSpeedX * (transform.position.x - initWorldSpaceX);
        rend.material.SetTextureOffset("_MainTex", new Vector2(offsetX, 0));
	}
}