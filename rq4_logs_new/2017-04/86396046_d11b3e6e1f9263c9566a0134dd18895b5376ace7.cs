using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FullScreenSprite : MonoBehaviour {

    public bool Variant = false;
    private SpriteRenderer sr;


    // Use this for initialization
    void Start() {
        sr = GetComponent<SpriteRenderer>();
        ResizeSpriteToScreen();
    }

    private void ResizeSpriteToScreen()
    {
        if (!Variant)
        {
            transform.localScale = new Vector3(1, 1, 1);

            float width = sr.sprite.bounds.size.x;
            float height = sr.sprite.bounds.size.y;

            float worldScreenHeight = Camera.main.orthographicSize * 2.0f;
            float worldScreenWidth = worldScreenHeight / Screen.height * Screen.width;


            worldScreenWidth /= width;
            worldScreenHeight /= height;

            if (worldScreenHeight < worldScreenWidth)
                worldScreenHeight = worldScreenWidth;
            else if (worldScreenHeight > worldScreenWidth)
                worldScreenWidth = worldScreenHeight;

            transform.localScale = new Vector3(worldScreenWidth, worldScreenHeight, 1);
        }
        else
        {
            transform.localScale = new Vector3(1, 1, 1);

            float width = sr.sprite.bounds.size.x;
            float height = sr.sprite.bounds.size.y;

            Debug.Log("Sprite W : " + width);
            Debug.Log("Sprite H : " + height);

            Debug.Log("Width : " + Screen.width);
            Debug.Log("Height : " + Screen.height);

            float worldScreenHeight = Screen.height;
            float worldScreenWidth = Screen.width;


            worldScreenWidth /= width;
            worldScreenHeight /= height;

            transform.localScale = new Vector3(worldScreenWidth, worldScreenHeight, 1);
        }
    }

    // Update is called once per frame
    void Update () {
		
	}
}