using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LevelControllerScript : MonoBehaviour
{
    public Camera GameCamera; //Takes a camera object as a parameter, but this is purely for relitive positioning. Assumes the camera has a FOV of 60;
    public float speed = 1.0f; //Speed of level;
    public static List<GameObject> BlockList = new List<GameObject>();
    private bool MakeDark = false;
    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        if (Time.frameCount % 60.0f == 0) //Create a cube every 1 seconds
        {
            if(MakeDark == true)
            {
                CreateBlock("dark");
                MakeDark = false;
            }
            else
            {
                CreateBlock("light");
                MakeDark = true;
            }
        }

        for (int i = 0; i < BlockList.Count; i++) //Keep track of the blocks
        {
            GameObject block = BlockList[i];
            if (block.transform.position.x <= GameCamera.transform.position.x - 20) //Check if any cubes need to be removed
            {
                DestroyBlock(block, i);
            }
            else
            {
                Vector3 currentPosition = block.transform.position;
                block.transform.position = new Vector3(currentPosition.x-speed, currentPosition.y, currentPosition.z); //Move each cube to the left
            }
        }
    }

    void CreateBlock(string color)
    {
        //Setup the cube object
        Vector3 CameraPosition = GameCamera.transform.position;
        GameObject block = GameObject.CreatePrimitive(PrimitiveType.Cube);
        Material matertial = block.GetComponent<Renderer>().material;

        //Set its color
        switch(color)
        {
            case ("light"):
                matertial.color = new Color(0.565f, 0.561f, 0.549f);
                break;
            case ("dark"):
                matertial.color = new Color(0.686f, 0.678f, 0.624f);
                break;
        }

        //Set its position and velocity
        block.transform.position = new Vector3(CameraPosition.x + 10.0f, CameraPosition.y - 2.2f, CameraPosition.z + 5.0f);

        //Add the block to the master list
        BlockList.Add(block);
    }

    void DestroyBlock(GameObject block, int blockIndex)
    {
        BlockList.RemoveAt(blockIndex);
        Destroy(block);
    }
}