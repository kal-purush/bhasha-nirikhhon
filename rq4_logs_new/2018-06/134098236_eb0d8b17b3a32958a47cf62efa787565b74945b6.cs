using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ThrowingArcRender : MonoBehaviour {

    LineRenderer lr;

    public float velocity;
    public float angle;
    public int resolution;

    float g;
    float radianAngle;

    Vector3 mousePos = new Vector3(0, 0, 0);
    public GameObject player;
    Vector3 playerPos = new Vector3(0, 0, 0);
    void Awake()
    {
        lr = GetComponent<LineRenderer>();
        g = Mathf.Abs(Physics2D.gravity.y);
    }

	// Use this for initialization
	void Start () {
        RenderArc();
	}
	
	// Update is called once per frame
	void Update () {
        playerPos = player.GetComponent<Transform>().position;
        mousePos = Input.mousePosition;
        mousePos = Camera.main.ScreenToWorldPoint(mousePos);
        //Debug.Log(mousePos);
        RenderArc();
    }

    void OnValidate()
    {
        if (lr != null && Application.isPlaying)
        {
            RenderArc();
        }
    }
    
    void RenderArc()
    {
        lr.SetVertexCount(resolution + 1);
        lr.SetPositions(CalculateArcArray());
    }

    Vector3[] CalculateArcArray()
    {
        Vector3[] arcArray = new Vector3[resolution + 1];

        
        float distance = Vector3.Distance(mousePos, playerPos);
        //radianAngle = Mathf.Deg2Rad * Vector3.Angle(mousePos, new Vector3(0, 0, -10));

        float thirdSide;
        float angle;
        if (mousePos.x < 0)
        {
            thirdSide = Vector3.Distance(mousePos, new Vector3(playerPos.x - distance, playerPos.y, playerPos.z));
            angle = Mathf.Acos((-2 * Mathf.Pow(distance, 2f) - Mathf.Pow(thirdSide, 2f)) / 2 * distance);
        }
        else
        {
            thirdSide = Vector3.Distance(mousePos, new Vector3(playerPos.x + distance, playerPos.y, playerPos.z));
            angle = Mathf.Acos((2 * Mathf.Pow(distance, 2f) - Mathf.Pow(thirdSide, 2f))/2*distance);
        }
        
        //Debug.Log(angle);
        radianAngle = angle;
        //((2maxD^2 - thirdside^2)/2maxD)inverseCos

        //Debug.Log(distance);
        //Debug.Log(radianAngle);


        for (int i = 0; i <= resolution; i++)
        {
            float t = (float)i / (float)resolution;
            if (playerPos.x == mousePos.x)
            {
                //arcArray[i] = CalculateArcPointStraightUp(t, Mathf.Abs(mousePos.y - playerPos.y));
            }
            else
            {
                arcArray[i] = CalculateArcPoint(t, mousePos.x - playerPos.x, Mathf.Abs(mousePos.y - playerPos.y));
            }
        }
        return arcArray;
    }

    Vector3 CalculateArcPoint(float t, float maxDistancex, float maxDistancey)
    {
        /*
        float x = t * maxDistance;
        float y = x * Mathf.Tan(radianAngle) - ((g * x * x) / (2 * velocity * velocity * Mathf.Cos(radianAngle) * Mathf.Cos(radianAngle)));
        return new Vector3(x, y);
        */

        float x = t * maxDistancex;
        float a = (playerPos.y - maxDistancey / 3) / Mathf.Pow(playerPos.x - maxDistancex / 3, 2f);
        float y;
        if (mousePos.y > playerPos.y)
        {
            y = a * Mathf.Pow(x - 2*maxDistancex / 3, 2f) + 4*maxDistancey / 3;
        }
        else
        {
            y = a * Mathf.Pow(x - maxDistancex / 3, 2f) + maxDistancey / 3;
        }
        
        return new Vector3(x, y);
    }

    Vector3 CalculateArcPointStraightUp(float t, float maxDistancey)
    {
        float y = 0;

        //TODO: straight line up or down
        //also make object follow each position
        //also make object stop following arc upon collision
            //use raycasts from each vertex on the arc?

        return new Vector3(playerPos.x, y);
    }

}