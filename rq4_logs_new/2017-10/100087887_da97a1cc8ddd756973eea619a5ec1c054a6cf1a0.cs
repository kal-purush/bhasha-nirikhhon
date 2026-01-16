using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CloudScript : MonoBehaviour {

    private int layer;
    private float moveSpeed, fallSpeed, opacity;
    private Vector2 size;

	// Update is called once per frame
	void Update () {

        this.transform.Translate(Vector2.right * moveSpeed * Time.deltaTime);
        this.transform.Translate(Vector2.down * fallSpeed * Time.deltaTime);

	}

    public void setupCloud()
    {
        if (layer == 0) { moveSpeed = 1.5f; fallSpeed = 3; this.GetComponent<SpriteRenderer>().sortingOrder = -5; opacity = 0.8f; }
        if (layer == 1) { moveSpeed = 1; fallSpeed = 2; this.GetComponent<SpriteRenderer>().sortingOrder = -6; opacity = 0.5f; }
        if (layer == 2) { moveSpeed = 0.5f; fallSpeed = 1; this.GetComponent<SpriteRenderer>().sortingOrder = -7; opacity = 0.3f; }

        if (layer == 0) { size = new Vector2(Random.Range(0.4f, 0.5f), Random.Range(0.4f, 0.5f)); }
        if (layer == 1) { size = new Vector2(Random.Range(0.3f, 0.4f), Random.Range(0.3f, 0.4f)); }
        if (layer == 2) { size = new Vector2(Random.Range(0.2f, 0.3f), Random.Range(0.2f, 0.3f)); }

        Color color = new Color(this.GetComponent<SpriteRenderer>().color.r,
            this.GetComponent<SpriteRenderer>().color.g,
            this.GetComponent<SpriteRenderer>().color.b, opacity);
        this.GetComponent<SpriteRenderer>().color = color;
        this.transform.localScale = size;
    }

    public void setLayer(int layer)
    {
        this.layer = layer;
        setupCloud();
    }
}