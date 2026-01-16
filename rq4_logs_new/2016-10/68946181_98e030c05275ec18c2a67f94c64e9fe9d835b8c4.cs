// Author: Miles
// Purpose: Resizes the background to fit the viewport.

using UnityEngine;
using System.Collections;

public class BackgroundResize : MonoBehaviour {

    void Start()
    {
        ResizeSpriteToScreen();
    }


    void ResizeSpriteToScreen()
    {
        SpriteRenderer sr = GetComponent<SpriteRenderer>();

        // Do nothing if null.
        if (sr == null)
            return;


        gameObject.GetComponent<Transform>().transform.localPosition = new Vector3(0, 0, 50);
        gameObject.GetComponent<Transform>().transform.localScale = new Vector3(1, 1, 1);

        // Get the needed variables
        float width = sr.sprite.bounds.size.x;
        float height = sr.sprite.bounds.size.y;
        float worldScreenHeight = Camera.main.orthographicSize * 2.0f;
        float worldScreenWidth = worldScreenHeight / Screen.height * Screen.width;

        // Resize the background
        gameObject.GetComponent<Transform>().transform.localScale = new Vector2(worldScreenWidth / width, worldScreenHeight / height);

    }
}