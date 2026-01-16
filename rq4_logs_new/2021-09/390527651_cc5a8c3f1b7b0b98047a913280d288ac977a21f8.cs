using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAnimator : MonoBehaviour 
{
    private Animator anim;

    private void Start()
    {
        anim = GetComponent<Animator>();
    }

    private void Update()
    {
        if (Input.GetKey(KeyCode.D))
        {
            anim.SetBool("isMoveRight", true);
        }
        else
        {
            anim.SetBool("isMoveRight", false);
        }

        if (Input.GetKey(KeyCode.A))
        {
            anim.SetBool("isMoveLeft", true);
        }
        else
        {
            anim.SetBool("isMoveLeft", false);
        }

        if (Input.GetKey(KeyCode.S) && !(Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.D)))
        {
            anim.SetBool("isMoveDown", true);
        }
        else
        {
            anim.SetBool("isMoveDown", false);
        }
        
        if (Input.GetKey(KeyCode.W) && !(Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.D)))
        {
            anim.SetBool("isMoveUp", true);
        }
        else
        {
            anim.SetBool("isMoveUp", false);
        }

        if (Input.GetKey(KeyCode.D) && Input.GetKey(KeyCode.A))
        {
            anim.SetBool("isMoveRight", false);
            anim.SetBool("isMoveLeft", false);
        }

        if (Input.GetKey(KeyCode.W) && Input.GetKey(KeyCode.S))
        {
            anim.SetBool("isMoveUp", false);
            anim.SetBool("isMoveDown", false);
        }
    }
}