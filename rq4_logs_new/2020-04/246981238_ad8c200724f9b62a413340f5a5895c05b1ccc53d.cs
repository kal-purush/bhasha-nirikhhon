using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class healthBarScript : MonoBehaviour
{
    Image HealthBar;
    float maxHealth = 100f;
    public static float health;

    // Start is called before the first frame update
    void Start()
    {
        HealthBar = GetComponent<Image>();
        health = maxHealth;
    }

    // Update is called once per frame
    void Update()
    {
        HealthBar.fillAmount = health / maxHealth;
        if (health <= 0)
        {
            Destroy(GameObject.FindGameObjectWithTag("Player"));
            Application.LoadLevel(0);
        }
    }
}