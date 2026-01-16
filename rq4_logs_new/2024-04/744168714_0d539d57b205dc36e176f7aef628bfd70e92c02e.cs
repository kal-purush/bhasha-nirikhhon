using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AlphaPulse_Sprite : MonoBehaviour
{
    private float pt, newVal;
    public float pulseSpeed = 0.1f;
    public float minAlpha, maxAlpha;
    public SpriteRenderer img;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        pt += pulseSpeed*Time.deltaTime;
        if (pt >= 6.28f)
        {
            pt = 0;
        }
        newVal = ((Mathf.Sin(pt)+1)*(maxAlpha - minAlpha))/2 + minAlpha;
        img = GetComponent<SpriteRenderer>();
        img.color = new Color (img.color.r, img.color.g, img.color.b, newVal);
    }
}