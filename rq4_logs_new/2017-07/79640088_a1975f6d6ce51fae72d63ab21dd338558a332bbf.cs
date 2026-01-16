using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[ExecuteInEditMode]
public class floorTiling : MonoBehaviour {


    public Material mat;
    //public float offset;
    public float yScale;

    public bool fixedSlices;
    public float numOfslices;

    public Renderer[] rends;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        if (!Application.isPlaying) {
            setup();
        }
	}

    void setup() {
        rends = GetComponentsInChildren<Renderer>();
        for (int i = 0; i < rends.Length; i++) {
            if (mat != null)
                rends[i].material = mat;

                if (!fixedSlices) {
                rends[i].material.SetTextureScale("_MainTex", new Vector2(1f / rends.Length, yScale));
                rends[i].material.SetTextureOffset("_MainTex", new Vector2(i * 1f / rends.Length, 0));
            } else {
                rends[i].material.SetTextureScale("_MainTex", new Vector2((i + 1f) / numOfslices, yScale));
                rends[i].material.SetTextureScale("_MainTex", new Vector2((i + 1f) / numOfslices, yScale));
            } 
            
        }
        //rend = GetComponent<Renderer>();
        //if (mat != null)
        //    rend.material = mat;

        //rend.material.SetTextureScale("_MainTex", new Vector2(scale, 1));
        //rend.material.SetTextureOffset("_MainTex", new Vector2(offset, 1));
    }
}