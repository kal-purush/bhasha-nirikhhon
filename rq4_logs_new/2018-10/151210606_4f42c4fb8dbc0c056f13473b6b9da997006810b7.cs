using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class diamondscript : MonoBehaviour
{
    [Range(-360, 360)]
    public float speed = 0;
    public SpriteRenderer render;
    public Color newColor;
    public Transform other;

    // Use this for initialization
    void Start()
    {
        render.color = newColor;
        render.color = new Color(1f, 0.5f, 0.3f);
        //render.flipY = true;
        //other.position = new Vector3(3, 2);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKey(KeyCode.D))
        {
            transform.Rotate(0, 0, -260 * Time.deltaTime);
        }

        if (Input.GetKey(KeyCode.A))
        {
            transform.Rotate(0, 0, 260 * Time.deltaTime);
        }

        if (Input.GetKey(KeyCode.W))
        {
            transform.Translate(5 * Time.deltaTime, 0, 0);
        }

        if (Input.GetKey(KeyCode.S))
        {
            transform.Translate(-5 * Time.deltaTime, 0, 0);
        }



      
    }
}