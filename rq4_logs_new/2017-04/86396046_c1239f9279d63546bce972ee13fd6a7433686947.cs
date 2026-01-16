using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VSButtonScript : MonoBehaviour {

    public VSCombinationHandler.Button Type;

    AudioSource audioSource;
    VSGameManager GM;
    Animator animator;

    // Use this for initialization
    void Start()
    {
        GM = VSGameManager.instance;
        animator = transform.GetComponentInChildren<Animator>();
        audioSource = GetComponent<AudioSource>();
    }

    public void ButtonPressed()
    {
        if (this.tag == "Player1")
            GM.ButtonPressed(Type, 1);

        if (this.tag == "Player2")
            GM.ButtonPressed(Type, 2);

        animator.SetTrigger("Pressed");
        audioSource.Play();
    }
}