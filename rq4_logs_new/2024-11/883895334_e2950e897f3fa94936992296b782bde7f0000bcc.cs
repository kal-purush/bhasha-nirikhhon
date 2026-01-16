public class PlayerController : MonoBehaviour
{
    private HealthSystem healthSystem;

    void Start()
    {
        healthSystem = GetComponent<HealthSystem>();
        healthSystem.onDeath.AddListener(OnPlayerDeath);
    }

    void OnPlayerDeath()
    {
        // Handle player death (e.g., show game over screen)
        Debug.Log("Player has died!");
    }

    // Example method to simulate taking damage
    public void SimulateDamage()
    {
        healthSystem.TakeDamage(10f);  // Simulate taking 10 damage
    }
}

public class Enemy : MonoBehaviour
{
    private HealthSystem healthSystem;

    void Start()
    {
        healthSystem = GetComponent<HealthSystem>();
        healthSystem.onDeath.AddListener(OnEnemyDeath);
    }

    void OnEnemyDeath()
    {
        // Handle enemy death (e.g., play death animation, destroy enemy)
        Debug.Log("Enemy has died!");
    }

    // Example method to simulate taking damage
    public void SimulateDamage()
    {
        healthSystem.TakeDamage(25f);  // Simulate taking 25 damage
    }
}