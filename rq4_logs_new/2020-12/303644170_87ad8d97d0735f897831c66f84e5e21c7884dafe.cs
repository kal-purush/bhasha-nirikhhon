using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Enemy : MonoBehaviour
{
    public int maxHealth = 100;
    public int currentHealth = 100;
    public Healthbar healthbar;
    private int damage = 20;
    public static bool isAttacking = false;

    Animator animator;

    // Start is called before the first frame update
    void Start()
    {
        currentHealth = maxHealth;
        healthbar.SetMaxHealth(maxHealth);
        animator = GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        if(isAttacking){
            animator.SetBool("isAttacking", true);
        } else{
            animator.SetBool("isAttacking", false);
        }
    }

    void TakeDamage(int damage){
        if(currentHealth > 0){
            currentHealth -= damage;
            healthbar.SetHealth(currentHealth);
        }
    }

    private void OnCollisionEnter2D(Collision2D other) {
        if(other.gameObject.tag.Equals("Player")){
            TakeDamage(damage);
            if(currentHealth <= 0){
                Destroy(gameObject);
            }
        }
    }
}