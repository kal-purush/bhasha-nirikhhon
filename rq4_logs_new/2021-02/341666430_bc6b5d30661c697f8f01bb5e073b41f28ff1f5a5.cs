using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class JumpBuff : MonoBehaviour
{
    public PlayerMovement movement;
    public float buffDuration = 1;
    public float jumpMultiplier = 1f;

    private bool _isTriggered;

    private void Update()
    {
        if (_isTriggered)
        {
            buffDuration -= Time.deltaTime;
            if (buffDuration <= 0)
            {
                JumpDecrease();
                Destroy(gameObject);
            }
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player") && !_isTriggered)
        {
            JumpIncrease();
            _isTriggered = true;
            GetComponent<SpriteRenderer>().enabled = false;
        }
    }

    private void JumpIncrease()
    {
        // Debug.Log("Skacze");
        movement.JumpMultiplier(jumpMultiplier);
    }

    private void JumpDecrease()
    {
        // Debug.Log("Nie Skacze");
        movement.JumpMultiplier(1 / jumpMultiplier);
    }
}