using System.Collections;
using UnityEngine;

public class CastleFoodTest : MonoBehaviour
{


    // START FUNCTION
    void Start()
    {
    }

    // UPDATE FUNCTION
    void Update()
    {

    }



    // GUI FUNCTION
    void OnGUI()
    {
        //Access the 'InReach' variable from raycasting script.
        GameObject Player = GameObject.Find("Player");
        Detection detection = Player.GetComponent<Detection>();

        if (detection.castleFoodInReach == true)
        {
            GUI.color = Color.white;
            GUI.Box(new Rect(20, 40, 200, 25), "Castle Food Test Cube");
        }
    }
}