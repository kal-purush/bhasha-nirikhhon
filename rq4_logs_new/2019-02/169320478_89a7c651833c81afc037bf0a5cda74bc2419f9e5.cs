using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bug : MonoBehaviour
{
    public GameObject bug;
    bool hasBeenClicked = false;
    Color defaultColor = Color.green;

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        checkForClick();

    }

    private void checkForClick()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Vector3 pointClicked = Camera.main.ScreenToWorldPoint(Input.mousePosition);
            Collider2D coll = bug.GetComponent<Collider2D>();

            if (coll.OverlapPoint(pointClicked) && !hasBeenClicked)
            {
                ScoreScript.scoreValue += 10;
                hasBeenClicked = true;
                SpriteRenderer spriteRenderer = bug.GetComponent<SpriteRenderer>();
                spriteRenderer.color = defaultColor;

            }
        }
    }
}