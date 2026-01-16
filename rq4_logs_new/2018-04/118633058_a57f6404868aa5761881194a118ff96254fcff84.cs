using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ToggleSwitch : MonoBehaviour {

    public GameObject Lock;
    LockObj myLock;
    Rigidbody2D rb2d;
    bool activated;
    float timer;
    Animator animator;
    // Use this for initialization
    void Start()
    {
        myLock = Lock.GetComponent("LockObj") as LockObj;
        rb2d = GetComponent<Rigidbody2D>();
        animator = GetComponent<Animator>();
        activated = false;
        timer = -1;
    }

    // Update is called once per frame
    void Update()
    {

    }
    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            myLock.Toggle();
            activated = !activated;
            animator.SetBool("active", activated);
        }
    }
}