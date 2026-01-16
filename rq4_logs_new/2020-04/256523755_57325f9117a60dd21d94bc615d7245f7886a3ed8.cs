using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class snsAnim : MonoBehaviour
{
    Animator anim;
    Vector3 lastPos = Vector3.zero;
    CharacterController control;
    Vector3 horizontalVelocity = Vector3.zero;
    public KeyCode run;
    bool isRun = false;
    
    // Start is called before the first frame update
    void Start()
    {
        anim = GetComponent<Animator>();
        control = GetComponent<CharacterController>();
    }

    // Update is called once per frame
    void LateUpdate()
    {
        horizontalVelocity = new Vector3(control.velocity.x, 0, control.velocity.z);
        float horizontalSpeed = horizontalVelocity.magnitude;
        //float verticalSpeed = verticalVelocity.magnitude;
        if (Input.GetKey(run))
        {
            isRun = true;
        }
        if (Input.GetKeyUp(run))
        {
            isRun = false;
        }

        if (horizontalSpeed > 0)
        {
            if (isRun)
            {
                anim.SetTrigger("run");
            }
            else
            {
                anim.SetTrigger("walk");
            }
        }




        if (horizontalVelocity.magnitude == 0)
        {
            anim.SetTrigger("idle");
        }
        if (Input.GetButtonDown("Jump"))
        {
            anim.SetTrigger("jump");
        }

        /*if (lastPos != gameObject.transform.position)
        {
            anim.SetTrigger("walk");
        }

        lastPos = gameObject.transform.position;*/

        //if (lastPos == gameObject.transform.position)
        //{
        //    anim.SetTrigger("idle");
        //}

    }
}