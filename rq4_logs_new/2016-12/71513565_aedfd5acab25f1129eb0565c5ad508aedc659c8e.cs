using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TriggerEvent : MonoBehaviour
{
    public AudioSource music;
    public bool playOnEnter = true;
    public bool triggerOnce = true;
    public bool musicStopOnExit = false;
    public bool stopMovement = false;
    public float timeToStopMovement = 0;
    public GameObject objectToActivate;

    PlayerController player;
    void Start()
    {
        player = FindObjectOfType<PlayerController>();
    }

    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.tag == "Player")
        {
            if (playOnEnter)
            {
                music.Play();
                if (stopMovement) {
                    player.canMove = false;
                    Invoke("activateMovement", timeToStopMovement);
                }
                if (objectToActivate)
                    objectToActivate.SetActive(true);
                if (triggerOnce)
                    gameObject.SetActive(false);
            }
        }
    }

    void OnTriggerExit2D(Collider2D other)
    {
        if (other.gameObject.tag == "Player")
        {
            if (musicStopOnExit)
            {
                music.Stop();
                if (triggerOnce)
                    gameObject.SetActive(false);
            }
        }
    }

    void activateMovement()
    {
        player.canMove = true;
    }
}