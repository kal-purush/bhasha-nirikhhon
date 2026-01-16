using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MusicButtonController : MonoBehaviour
{
    public Animator Anim;
    public float index;

    void Awake()
    {

    }

    void Start()
    {

    }

    void Update()
    {
        ButtonAnimation();
    }

    private void ButtonAnimation()
    {
        if(Input.GetKey("space"))
        {
            Anim.enabled = true;
            Anim.SetBool("press", true);
        }
        else{
            Anim.SetBool("press", false);
            if(index < 5 && Input.GetKeyDown("down"))
            {
                transform.position -= new Vector3(0, 53f, 0f);
                index++;
            }
            else if(index > 1 && Input.GetKeyDown("up"))
            {
                transform.position += new Vector3(0, 53f, 0f);
                index--;
            }
        }
        
    }

}