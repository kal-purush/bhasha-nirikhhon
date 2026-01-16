// script for controlling damage caused by an enemy
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyStrike : MonoBehaviour
{
    [SerializeField] Weapon strikingWeapon;
    private SpriteRenderer spriteRenderer;
    
    // Start is called before the first frame update
    void Start()
    {
        spriteRenderer = GetComponent<SpriteRenderer>();
        spriteRenderer.sprite = strikingWeapon.icon;
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.CompareTag("Player"))
        {
            PlayerController player = collision.gameObject.GetComponent<PlayerController>();
            player.health -= strikingWeapon.damage;
            Debug.Log($"Player health is {player.health}");
        } else if(collision.gameObject.CompareTag("Wolf"))
        {
            WolfAgentMovement wolf = collision.gameObject.GetComponent<WolfAgentMovement>();
            wolf.health -= strikingWeapon.damage;
        }
    }

}