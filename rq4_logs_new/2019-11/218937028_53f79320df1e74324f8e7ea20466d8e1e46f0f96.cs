using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AnimationController : MonoBehaviour
{
    private Animator animator;

    void Awake() {
        animator = GetComponent<Animator>();
    }

    public void walk() {
        animator.SetTrigger("Walk");
        Debug.Log("d");
    }

    public void hello() {
        animator.SetTrigger("Hello");
    }

    public void surprise() {
        animator.SetTrigger("Surprise");
    }

    public void dancing() {
        animator.SetTrigger("Dancing");
    }
}