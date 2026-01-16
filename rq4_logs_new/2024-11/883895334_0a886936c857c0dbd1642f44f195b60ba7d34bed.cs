using UnityEngine;

public class Health : MonoBehaviour
{
    public int maxHealth = 100;
    public int currentHealth;
    public HealthUI healthUI; // Reference to the HealthUI script

    void Start()
    {
        currentHealth = maxHealth;
        if (healthUI != null)
        {
            healthUI.UpdateHealth(currentHealth, maxHealth); // Initialize UI
        }
    }

    public void TakeDamage(int damage)
    {
        currentHealth -= damage;
        currentHealth = Mathf.Clamp(currentHealth, 0, maxHealth);

        if (healthUI != null)
        {
            healthUI.UpdateHealth(currentHealth, maxHealth); // Update UI
        }

        if (currentHealth <= 0)
        {
            Die();
        }
    }

    public void Heal(int amount)
    {
        currentHealth += amount;
        currentHealth = Mathf.Clamp(currentHealth, 0, maxHealth);

        if (healthUI != null)
        {
            healthUI.UpdateHealth(currentHealth, maxHealth); // Update UI
        }
    }

    void Die()
    {
        Debug.Log("Player is Dead!");
        // Add death logic here
    }
}