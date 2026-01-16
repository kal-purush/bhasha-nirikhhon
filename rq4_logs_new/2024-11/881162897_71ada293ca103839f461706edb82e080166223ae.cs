using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AnimationController : MonoBehaviour
{
    Animator ani;
    bool crouching = false;

    // Start is called before the first frame update
    void Start()
    {
        ani= GetComponent<Animator>();  
    }

    // Update is called once per frame
    void Update()
    {
        
        if(Input.GetKeyDown(KeyCode.Space))
        {
            ani.SetTrigger("jump");  
        }
        if(Input.GetKeyUp(KeyCode.C))
        {
            crouching = !crouching;
            ani.SetBool("crouching", crouching);
        }
    }
}