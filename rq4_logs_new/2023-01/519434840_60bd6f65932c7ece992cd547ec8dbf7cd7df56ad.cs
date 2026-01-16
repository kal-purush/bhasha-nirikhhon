using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HandStop : MonoBehaviour
{
    private PlayerController player;
    private float playerSpeed;

    void Start()
    {
        FindObjectOfType<AudioManager>().PlaySound("Bone");
        player = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerController>();
        playerSpeed = player.MoveSpeed;
        StartCoroutine(DelayedBoneSound());
    }

    private void OnTriggerEnter2D(Collider2D other) 
    {
        if (other.gameObject.tag == "Player")
        {
            player.MoveSpeed = 0f;
            StartCoroutine(ReleaseHand());
        }
    }
    

    private IEnumerator ReleaseHand()
    {
        yield return new WaitForSeconds(2f);
        player.MoveSpeed += playerSpeed;
        yield return null;
    }
    
    private IEnumerator DelayedBoneSound()
    {
        yield return new WaitForSeconds(2.5f);
        FindObjectOfType<AudioManager>().PlaySound("Bone");
        yield return new WaitForSeconds(4.5f);
        FindObjectOfType<AudioManager>().PlaySound("Bone");
        yield return null;
    }

    
}