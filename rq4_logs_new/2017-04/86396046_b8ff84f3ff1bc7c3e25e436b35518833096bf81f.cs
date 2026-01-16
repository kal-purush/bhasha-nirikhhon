using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class EntityUIFitScreen : MonoBehaviour {

    public float PercentageButtonFit = 50.0f;
    SpriteRenderer sr;
    RectTransform rt;
    GameObject ButtonEntity;
   // public Text debugT;
    bool _isInit = false;

	// Use this for initialization
	void Start () {
        sr = GetComponentInChildren<SpriteRenderer>();
        rt = GetComponentInParent<RectTransform>();
	}

    void ChangeSize()
    {
        string debug;

        float sizeBackgroundX = rt.rect.size.x;
        float sizeBackgroundY = rt.rect.size.y;
        debug = "Size Background : " + sizeBackgroundX + " " + sizeBackgroundY + "\n";
        Debug.Log("Size Background : " + sizeBackgroundX + " " + sizeBackgroundY);

        float sizeSpriteX = sr.sprite.bounds.size.x;
        float sizeSpriteY = sr.sprite.bounds.size.y;
        debug += "Size Sprite : " + sizeSpriteX + " " + sizeSpriteY + "\n";
        Debug.Log("Size Sprite : " + sizeSpriteX + " " + sizeSpriteY);

        float PercenTageSpriteX = sizeSpriteX * 100.0f / sizeBackgroundX;
        float PercenTageSpriteY = sizeSpriteY * 100.0f / sizeBackgroundY;
        debug += "Percentage Sprite : " + PercenTageSpriteX + " " + PercenTageSpriteY + "\n";
        Debug.Log("Percentage Sprite : " + PercenTageSpriteX + " " + PercenTageSpriteY);

        float scaleX = PercentageButtonFit / PercenTageSpriteX;//Mathf.Floor(PercentageButtonFit / PercenTageSpriteX * 10.0f) / 10.0f;
        float scaleY = PercentageButtonFit / PercenTageSpriteY;//Mathf.Floor(PercentageButtonFit / PercenTageSpriteY * 10.0f) / 10.0f;
        Debug.Log("Scale : " + scaleX + " " + scaleY);
        debug += "Scale : " + scaleX + " " + scaleY + "\n";

        // Make the scale uniform
        if (scaleX < scaleY) scaleY = scaleX;
        else if (scaleY < scaleX) scaleX = scaleY;
        Debug.Log("Uni Scale : " + scaleX + " " + scaleY);

        transform.localScale = new Vector3(scaleX, scaleY, 1.0f);
        transform.localPosition = new Vector3(sizeBackgroundX / 2.0f, sizeBackgroundY / 2.0f, 1.0f);
        //debugT.text = debug;
        _isInit = true;
    }
	
	// Update is called once per frame
	void Update () {
        if (!_isInit)
            ChangeSize();
        Debug.Log(transform.localScale);
    }
}