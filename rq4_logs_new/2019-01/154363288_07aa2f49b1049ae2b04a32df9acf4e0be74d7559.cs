using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_ExtractorMat : MonoBehaviour
{
    public Material material;
    public float lerp;
    private float t;

    // Start is called before the first frame update
    void Start()
    {
        //Fetch the Material from the Renderer of the GameObject
        material = GetComponent<Renderer>().material;
        print("Materials " + Resources.FindObjectsOfTypeAll(typeof(Material)).Length);
        //InvokeRepeating("UpdateMat", 3, 0.5f);
        Debug.Log("Cambio");
        lerp = 1;
    }

    // Update is called once per frame
    void Update()
    {
        
        //lerp = Mathf.Lerp(0, 1, t);
        if (Input.GetKey(KeyCode.K))
        {
            lerp -= 0.01f;
            //material.SetFloat("_SliceAmount", 0.5f + Mathf.Sin(Time.time) * 0.5f);
            material.SetFloat("_SliceAmount", lerp);
            Debug.Log("Cambio");
        }
    }
    void UpdateMat()
    {
        //material.SetFloat("_SliceAmount", 0.5f + Mathf.Sin(Time.time) * 0.5f);
        Debug.Log("Cambio");
    }
}