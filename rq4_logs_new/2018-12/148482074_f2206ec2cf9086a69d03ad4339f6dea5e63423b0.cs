using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

public class ColliderEvents : MonoBehaviour {

    public Animator anim;
    public string parameterSet;
    public string complimentParameterSet;

    private void OnTriggerStay2D(Collider2D collision)
    {
        if(collision.GetComponent<PlayerController>())
        {
            anim.SetBool(parameterSet, true);
            anim.SetBool(complimentParameterSet, false);
        }
    }
}