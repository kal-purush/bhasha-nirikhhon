using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class LifeBehaviour : MonoBehaviour
{
    [SerializeField]
    private int maxHealth = 100;
    [SerializeField]
    private Slider slider = null;

    private int currentHealth = 0;

    // Start is called before the first frame update
    void Start()
    {
        currentHealth = maxHealth;
    }

    // Update is called once per frame
    void Update()
    {

    }

    /// <summary>
    /// Adds an amount of life from a GameObject.
    /// </summary>
    /// <param name="amount">Amount of life to add.</param>
    public void OnLifeGain (int amount)
    {
        currentHealth = (amount + currentHealth > maxHealth) ? maxHealth : currentHealth + amount;

        slider.value = currentHealth;
    }

    /// <summary>
    /// Subtract an amount of life from a GameObject.
    /// </summary>
    /// <param name="amount">Amount of life to subtract.</param>
    public void OnLifeLoss (int amount)
    {
        currentHealth = (currentHealth - amount > 0) ? currentHealth - amount : 0;

        slider.value = currentHealth;
    }

}