using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent (typeof (RectTransform))]
public class Bar : MonoBehaviour {

    private RectTransform rectTransform;
    private float maxWidth;

	// Use this for initialization
	void Start () {

        rectTransform = GetComponent<RectTransform>();
        maxWidth = rectTransform.sizeDelta.x;


    }
	
    public void SetSize (float percentFilled) {

        rectTransform.sizeDelta = new Vector2(maxWidth * percentFilled, rectTransform.sizeDelta.y);

    }

}