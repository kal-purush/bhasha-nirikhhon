using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMove : MonoBehaviour
{
    //basic variables
    private GameObject player;
    public float x_min;
    public float x_max;
    public float y_min;
    public float y_max;

    // Start is called before the first frame update
    void Start()
    {
        
        player = GameObject.Find("PlayerGirl");

    }

    // Update is called once per frame
    void LateUpdate()
    {
        float x = Mathf.Clamp(player.transform.position.x, x_min, x_max);
        float y = Mathf.Clamp(player.transform.position.y, y_min, y_max);
        gameObject.transform.position = new Vector3(x, y, gameObject.transform.position.z);

    }
}