using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BoyMoveTrigger : MonoBehaviour {
    public Transform boy;
    public float boySpeed = 3;

    public float howLongToWalk = 2;
    public bool triggerOnce = true;
    public bool up = false;
    public bool down = false;
    public bool left = false;
    public bool right = false;
    bool beenTrigger = false;
    bool walk = false;

    Rigidbody2D boyRigidbody;
    Animator boyAnimator;
    // Use this for initialization
    void Start () {
        boyRigidbody = boy.GetComponent<Rigidbody2D>();
        boyAnimator = boy.GetComponent<Animator>();
    }

    // Update is called once per frame
    void FixedUpdate () {
        Debug.Log(walk);
        if (walk)
        {
            if (up) {
                boyRigidbody.velocity = new Vector2(0, boySpeed);
                boyAnimator.SetFloat("walkY", boySpeed);
            }
            else if (down) {
                boyRigidbody.velocity = new Vector2(0, -boySpeed);
                boyAnimator.SetFloat("walkY", -boySpeed);

            }
            else if (right)
            {
                boyRigidbody.velocity = new Vector2(boySpeed, 0);
                boyAnimator.SetFloat("walkX", boySpeed);
            }
            else if (left)
            {
                boyRigidbody.velocity = new Vector2(-boySpeed, 0);
                boyAnimator.SetFloat("walkX", -boySpeed);
            }

        }
    }

    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.tag == "Player" && !beenTrigger)
        {
            Invoke("Stop", howLongToWalk);
            walk = true;
         

            if (triggerOnce)
                beenTrigger = true;
        }
    }

    void Stop()
    {
        walk = false;
        boy.gameObject.SetActive(false);
        if (triggerOnce)
            gameObject.SetActive(false);
    }
}