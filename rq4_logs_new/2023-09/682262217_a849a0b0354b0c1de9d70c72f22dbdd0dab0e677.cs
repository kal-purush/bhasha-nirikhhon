using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Minerals : MonoBehaviour
{
    public float maxHealth = 10;
    public float currentHealth;
    public List<GameObject> worms;

    private void Awake()
    {
        maxHealth = 10;
    }
    void Start()
    {
        currentHealth = maxHealth;
    
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void takeDamage()
    {


        if (Input.GetKeyDown(KeyCode.Space))
        {
            currentHealth -= 1;
            print currentHealth;
        }
    }
}