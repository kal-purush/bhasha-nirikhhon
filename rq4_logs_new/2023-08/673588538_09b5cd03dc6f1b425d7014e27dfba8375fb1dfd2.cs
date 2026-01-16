using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.SocialPlatforms.Impl;
using UnityEngine.UI;

public class BumperInteractionTriangle : MonoBehaviour
{
    [SerializeField]
    Canvas scoreCanvas;
    [SerializeField]
    float minForce;
    [SerializeField]
    float maxForce;

    public static int score = 0;
    // Force parameters (will be randomised)
    float forceValue;
    Vector3 force;
    // Animator
    Animator animator;

    // Start is called before the first frame update    
    void Start()
    {
        score = 0;
        animator = GetComponent<Animator>();
    }
    // Triggered when something collides with the object
    private void OnCollisionEnter(Collision collision)
    {
        // If collision with a ball object
        if (collision.gameObject.tag == "ball")
        {
            Debug.Log("Ball hit");
            // Force value is random between minForce and maxForce
            forceValue = Random.Range(minForce, maxForce);
            // Debug.Log("Force value: " + forceValue);
            // Debug.Log(collision.contacts[0].normal);
            // Add force opposite to collision (opposite to first collision point normal)
            force = -collision.contacts[0].normal;
            // Raycast showing force direction
            // Debug.DrawRay(collision.contacts[0].point, force * 10, new Color(255, 0, 0), 10f);
            collision.rigidbody.AddForce(force * forceValue);
            // Add to the score
            ScoreKeeper sk = scoreCanvas.GetComponent<ScoreKeeper>();
            sk.scoreAdd();
            // Animation trigger
            animator.SetTrigger("BallHit");
        }
    }
}