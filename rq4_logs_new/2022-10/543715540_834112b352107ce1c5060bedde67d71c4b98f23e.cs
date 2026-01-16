using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HealthSystem : MonoBehaviour
{
    public int maxHealth;
    private int currentHealth;
    public Slider bossHealth;
    Animator animator;

    void Start()
    {
        this.currentHealth = maxHealth;
        this.bossHealth.value = this.getPercentage();
        animator = GetComponent<Animator>();
    }

    public void damage(int amount)
    {
        this.currentHealth -= amount;
        if (this.currentHealth <= 0)
        {
            this.currentHealth = 0;
            animator.SetBool("isDead", true);
        }
        this.bossHealth.value = this.getPercentage();
    }

    public float getPercentage()
    {
        return (float)this.currentHealth / (float)this.maxHealth * 100;

    }

    public bool isDead()
    {
        return this.currentHealth == 0;
    }

    void Update()
    {
        if (Input.GetKeyDown(KeyCode.F))
        {
            this.damage(10);
        }
    }

}