using UnityEngine;
using UnityEngine.UI; // For displaying the health bar

public class PlayerHealth : MonoBehaviour
{
    public int maxHealth = 100; // Maximum health
    public int currentHealth;  // Current health

    public Slider healthBar; // Reference to the health bar UI slider

    void Start()
    {
        currentHealth = maxHealth; // Initialize health to max
        UpdateHealthBar();         // Set the health bar to full
    }

    public void TakeDamage(int damage)
    {
        currentHealth -= damage; // Reduce health
        currentHealth = Mathf.Clamp(currentHealth, 0, maxHealth); // Prevent negative health
        UpdateHealthBar();       // Update the UI

        if (currentHealth <= 0)
        {
            Die(); // Handle player death
        }
    }

    public void Heal(int amount)
    {
        currentHealth += amount; // Increase health
        currentHealth = Mathf.Clamp(currentHealth, 0, maxHealth); // Prevent health from exceeding max
        UpdateHealthBar();       // Update the UI
    }

    void UpdateHealthBar()
    {
        if (healthBar != null)
        {
            healthBar.value = (float)currentHealth / maxHealth; // Update health bar proportionally
        }
    }

    void Die()
    {
        Debug.Log("Player is Dead!"); // Placeholder for death logic
        // Add death logic here, such as displaying a game-over screen
    }
}