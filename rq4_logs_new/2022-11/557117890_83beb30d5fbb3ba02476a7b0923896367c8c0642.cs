using System.Collections;
using System.Collections.Generic;
using UnityEngine;

//https://www.youtube.com/watch?v=ih8gyGeC7xs

public class FootstepScript : MonoBehaviour
    {
        private CharacterController myController;
        private AudioSource footsteps;

    void Start()

    {
        myController = GetComponent<CharacterController>();
        footsteps = GetComponent<AudioSource>();
    }

    void Update()
    {
        if (myController.isGrounded == true && myController.velocity.magnitude > 2f && footsteps.isPlaying == false)
        {
            if (Input.GetKey(KeyCode.W) || Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.S) || Input.GetKey(KeyCode.D))
            footsteps.Play();
            footsteps.volume = Random.Range(0.8f, 1);
            footsteps.pitch = Random.Range(0.9f, 1.1f);
        }
        if (myController.velocity.magnitude <= 0) {
            footsteps.Stop();
        }
    }
}