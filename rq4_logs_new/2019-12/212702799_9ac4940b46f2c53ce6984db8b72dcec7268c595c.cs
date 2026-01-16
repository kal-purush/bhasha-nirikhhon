using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Parallaxing : MonoBehaviour
{

    public Transform[] background; // arrat of all back and foregrounds to be parallaxed
    private float[] parallaxedScales; //proportion of cameras movement
    public float smoothing = 1; // how smooth parallax is... This needs to be bigger than 0
    private Transform cam;
    private Vector3 previousCamPosition; // position of camera in previous frame
    
     // awake is called before start()
    void Awake()
    {
        // setup the camera reference
        cam = Camera.main.transform;
    }
    // Start is called before the first frame update
    void Start()
    {
        // the previous frame had the current frames camera position
        previousCamPosition = cam.position;
        parallaxedScales = new float[background.Length]; 
        
        // assigning parallax scales
        for(int i = 0; i < background.Length; i++)
        {
            parallaxedScales[i] = background[i].position.z*-1;
        }
    }

    // Update is called once per frame
    void Update()
    {
        // for each backgroud
        for(int i = 0; i < background.Length; i++)
        {
            // parallax is opposite of camera movement
            float parallax = (previousCamPosition.x - cam.position.x) * parallaxedScales[i];
            
            // set a target x position which is the current position plus the parallax
            float backgroudTargetPosX = background[i].position.x + parallax;

            //create a target position which is backgrounds current pos plus its target x position
            Vector3 backgroudTargetPos = new Vector3 (backgroudTargetPosX, background[i].position.y, background[i].position.z);

            // fade between current position and target position using lerp
            background[i].position = Vector3.Lerp (background[i].position, backgroudTargetPos, smoothing * Time.deltaTime);
        }

        previousCamPosition = cam.position;
    }


}