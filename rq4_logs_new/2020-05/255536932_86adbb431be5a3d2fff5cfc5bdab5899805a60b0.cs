using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class buildCity : MonoBehaviour
{
    
    public GameObject[] buildings;
    public int mapWidth = 20;
    public int mapHeight = 20;
    int buildingFootprint = 60;
    //12:11 in tutorial for now
    
    // Start is called before the first frame update
    void Start()
    {

        float seed = 96;//Random.Range(0, 100);
        Debug.Log("seed = " + seed);

        for (int h = 0; h < mapHeight; h++)
        {
            for (int w = 0; w < mapWidth; w++)
            {
                
                

                float heightOffset = 0.0f;
                float xRotateOffset = 0.0f;
                float yRotateOffset = 0.0f;
                float zRotateOffset = 0.0f;
                
                
                Quaternion rot = Quaternion.identity;
                rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);

                Vector3 pos = new Vector3((w * buildingFootprint),heightOffset,(h * buildingFootprint));

                

                int result = (int)(Mathf.PerlinNoise(w/10.0f + seed, h/10.0f + seed) * 12);

                if (result < 2)
                {
                    buildingFootprint = 60;
                    heightOffset = 17.3f;
                    xRotateOffset = 0.0f;
                    yRotateOffset = 0.0f;
                    zRotateOffset = 0.0f;
                    pos = new Vector3((w * buildingFootprint), heightOffset, (h * buildingFootprint));
                    rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);
                    Instantiate(buildings[0], pos, rot);
                }
                    
                else if (result < 3)
                {
                    buildingFootprint = 60;
                    heightOffset = 0.71f;
                    xRotateOffset = -90.0f;
                    yRotateOffset = 0.0f;
                    zRotateOffset = 0.0f;
                    pos = new Vector3((w * buildingFootprint), heightOffset, (h * buildingFootprint));
                    rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);
                    Instantiate(buildings[2], pos, rot);                    
                }
                    
                else if (result < 5)
                {
                    buildingFootprint = 60;
                    heightOffset = 0.1f;
                    xRotateOffset = -90.0f;
                    yRotateOffset = 90.0f;
                    zRotateOffset = 0.0f;
                    pos = new Vector3((w * buildingFootprint), heightOffset, (h * buildingFootprint));
                    rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);
                    Instantiate(buildings[3], pos, rot);                    
                }
                    
                else if (result < 8)
                {
                    buildingFootprint = 60;
                    heightOffset = 9.26f;
                    xRotateOffset = 0.0f;
                    yRotateOffset = 0.0f;
                    zRotateOffset = 0.0f;
                    pos = new Vector3((w * buildingFootprint), heightOffset, (h * buildingFootprint));
                    rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);
                    Instantiate(buildings[1], pos, rot);                    
                }
                
                else if (result < 9)
                {
                    buildingFootprint = 60;
                    heightOffset = 1.0f;
                    xRotateOffset = 0.0f;
                    yRotateOffset = 0.0f;
                    zRotateOffset = 0.0f;
                    pos = new Vector3((w * buildingFootprint), heightOffset, (h * buildingFootprint));
                    rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);
                    Instantiate(buildings[4], pos, rot);
                }
                
                else if (result < 12)
                {
                    buildingFootprint = 60;
                    heightOffset = 1.0f;
                    xRotateOffset = 0.0f;
                    yRotateOffset = 0.0f;
                    zRotateOffset = 0.0f;
                    pos = new Vector3((w * buildingFootprint), heightOffset, (h * buildingFootprint));
                    rot.eulerAngles = new Vector3(xRotateOffset, yRotateOffset, zRotateOffset);
                    Instantiate(buildings[5], pos, rot);
                }
            }
        }
    }

    // Update is called once per frame
    /*
    void Update()
    {
        
    }
    */
}