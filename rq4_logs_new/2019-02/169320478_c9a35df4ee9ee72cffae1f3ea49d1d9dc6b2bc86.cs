using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BugClicking : MonoBehaviour
{
    public GameObject bug;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {

        if (Input.GetMouseButtonDown(0))
        {
            Vector3 pointClicked = Camera.main.ScreenToWorldPoint(Input.mousePosition);
            Collider2D coll = bug.GetComponent<Collider2D>();

            if (coll.OverlapPoint(pointClicked))
            {
                ScoreScript.scoreValue += 10;
            }
        }
    }
}