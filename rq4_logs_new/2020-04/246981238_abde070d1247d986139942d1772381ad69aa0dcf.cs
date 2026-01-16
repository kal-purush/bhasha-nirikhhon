using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class player : MonoBehaviour
{
    public int maxHealth = 6;
    public int currentHealth;

    public healthBarScript healthBar;
    // Start is called before the first frame update
    void Start()
    {
        currentHealth = maxHealth;
        healthBar.setMaxHealth(maxHealth);
    }

    // Update is called once per frame
    void Update()
    {
        if(Input.GetKeyDown(KeyCode.H))
        {
            TakeDamage();
        }
    }
    void TakeDamage()
    {
        currentHealth-=1;
        healthBar.setHealth(currentHealth);
    }
}