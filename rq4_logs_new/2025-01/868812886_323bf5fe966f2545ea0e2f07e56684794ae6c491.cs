using UnityEngine;

public class HealthPickup : MonoBehaviour
{
    PlayerHealth playerHealth;
    public int healthBonus = 20;

    void Awake()
    {
        playerHealth = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerHealth>();
    }

    void OnTriggerEnter(Collider other)
    {
        if(playerHealth.currentHealth < playerHealth.maxHealth)
        {
            Destroy(gameObject);
            print("Health boost picked up!");
            playerHealth.GetComponent<PlayerHealth>().IncreaseHealth(healthBonus);
            print("Current health: " + playerHealth.currentHealth);
        }
    }
}