using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Highlightable : MonoBehaviour {

    private Color myColor;
    public Color highlightColor;

    // Use this for initialization
    void Start () {
        myColor = gameObject.GetComponent<Renderer>().material.color;
    }

    private void OnMouseEnter()
    {
        gameObject.GetComponent<Renderer>().material.color = highlightColor;
    }

    private void OnMouseExit()
    {
        gameObject.GetComponent<Renderer>().material.color = myColor;
    }

}