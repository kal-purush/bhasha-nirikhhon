using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraPanning : MonoBehaviour {
    GameObject rocketGO;
    public float heightOffset = -100, verticalPadding = 30;
    public string bottomObject = "Rocket", topObject = "End Planet";
    float lowerY, upperY;

    bool panning = false;
    Vector3 mousePosition;

    void setToPosition(float x, float y)
    {
        gameObject.transform.position = new Vector3(x, y, heightOffset);
    }

    void movePosition(float dx, float dy)
    {
        gameObject.transform.position += new Vector3(dx, dy, 0);
    }

	// Use this for initialization
	void Start () {
        rocketGO = GameObject.Find("Rocket");
        lowerY = GameObject.Find(bottomObject).transform.position.y + verticalPadding;
        upperY = Mathf.Max(GameObject.Find(topObject).transform.position.y - verticalPadding,lowerY);
        Debug.Log(lowerY);
        setToPosition(0, lowerY);
	}


    // Update is called once per frame
    void Update()
    {
        if (!GameManager.isLaunched)
        {
            if (GameManager.isComputer)
            {
                if (Input.GetMouseButtonDown(0) && !GameManager.mouseOccupied)
                {
                    panning = true;
                    mousePosition = Input.mousePosition;
                }
                if (Input.GetMouseButton(0) && panning)
                {
                    //Debug.Log(Input.mousePosition);
                    Vector3 delta = 0.3f * (mousePosition - Input.mousePosition), curPosition = gameObject.transform.position;
                    float xMultiplier = 1 / (1 + Mathf.Abs(curPosition.x)), yMultiplier;

                    if (curPosition.y < lowerY) yMultiplier = 1 / (1 + lowerY - curPosition.y);
                    else if (curPosition.y > upperY) yMultiplier = 1 / (1 + curPosition.y - upperY);
                    else yMultiplier = 1;

                    movePosition(xMultiplier * delta.x, yMultiplier * delta.y);
                    mousePosition = Input.mousePosition;
                }
                if (Input.GetMouseButtonUp(0))
                {
                    panning = false;
                }
            }
            if (!panning)
            {
                if (gameObject.transform.position.x != 0)
                {
                    int sign = (gameObject.transform.position.x == Mathf.Abs(gameObject.transform.position.x)) ? -1 : 1;
                    movePosition(sign * Mathf.Min(Mathf.Abs(gameObject.transform.position.x),
                        20 / (1 + Mathf.Abs(gameObject.transform.position.x))), 0);
                }
                if (gameObject.transform.position.y < lowerY)
                {
                    movePosition(0, Mathf.Min(lowerY - gameObject.transform.position.y,
                        20 / (1 + lowerY - gameObject.transform.position.y)));
                }
                else if (gameObject.transform.position.y > upperY)
                {
                    movePosition(0, Mathf.Max(upperY - gameObject.transform.position.y,
                        -20 / (1 + gameObject.transform.position.y - upperY)));
                }
            }
        }
        else
        {
            setToPosition(0, Mathf.Min(upperY, Mathf.Max(lowerY,rocketGO.transform.position.y)));
        }
    }
}