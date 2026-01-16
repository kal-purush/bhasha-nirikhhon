using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EndingGate : MonoBehaviour {

    public Animator animator;

    // Use this for initialization
    void Start () {
        SceneTransition.setAnimator(animator);
        SceneTransition.setScene(""); 
	}

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag.Equals("Player") || other.gameObject.tag.Equals("PlayerFace"))
        {
            StartCoroutine(SceneTransition.LoadScene());
        }
    }
}