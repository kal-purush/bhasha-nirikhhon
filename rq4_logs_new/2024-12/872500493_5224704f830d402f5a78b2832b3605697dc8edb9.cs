using UnityEngine;
using UnityEngine.SceneManagement;

public class BossController : MonoBehaviour
{
    [Header("Boss Stats")]
    public int maxHealth = 5;
    public float shootInterval = 2f;
    public float projectileSpeed = 8f;

    [Header("References")]
    [SerializeField] private GameObject projectilePrefab; // Added SerializeField
    [SerializeField] private Transform shootPoint;
    public GameObject portalPrefab;

    private int currentHealth;
    private float shootTimer;
    private bool isDead;
    private Transform player;
    private bool isShootingEnabled = true;

    private void Start()
    {
        currentHealth = maxHealth;
        player = GameObject.FindGameObjectWithTag("Player")?.transform;
        
        // Validate required components
        if (!projectilePrefab)
        {
            Debug.LogError($"{gameObject.name}: Missing Projectile Prefab reference!");
            enabled = false;
            return;
        }

        if (!shootPoint)
        {
            Debug.LogError($"{gameObject.name}: Missing Shoot Point reference!");
            enabled = false;
            return;
        }
    }

    private void Update()
    {
        if (isDead) return;

        if (player == null)
        {
            player = GameObject.FindGameObjectWithTag("Player")?.transform;
            if (player == null) return; // Wait until player is found
        }

        shootTimer += Time.deltaTime;
        
        if (shootTimer >= shootInterval)
        {
            ShootAtPlayer();
            shootTimer = 0f;
        }
    }

    private void ShootAtPlayer()
    {
        // Validate references
        if (player == null)
        {
            Debug.LogWarning("Player reference is null");
            return;
        }
        if (projectilePrefab == null)
        {
            Debug.LogWarning("Projectile Prefab reference is null");
            return;
        }
        if (shootPoint == null)
        {
            Debug.LogWarning("Shoot Point reference is null");
            return;
        }

        // Instantiate projectile at shootPoint position
        GameObject projectile = Instantiate(projectilePrefab, shootPoint.position, Quaternion.identity);
        if (projectile == null)
        {
            Debug.LogError("Failed to instantiate projectile");
            return;
        }

        // Get the Rigidbody2D component
        Rigidbody2D rb = projectile.GetComponent<Rigidbody2D>();
        if (rb == null)
        {
            Debug.LogError("Projectile prefab missing Rigidbody2D component");
            Destroy(projectile);
            return;
        }

        // Calculate direction towards player and set velocity
        Vector2 direction = (player.position - shootPoint.position).normalized;
        rb.velocity = direction * projectileSpeed;
        Debug.Log($"Projectile fired at speed: {projectileSpeed}");
    }


    public void TakeDamage()
    {
        if (isDead) return;
        currentHealth--;
        if (currentHealth <= 0)
        {
            Die();
        }
    }


    private void Die()
    {
        isDead = true;
        isShootingEnabled = false;

        if (portalPrefab != null)
        {
            Instantiate(portalPrefab, transform.position, Quaternion.identity);
        }

        // StartCoroutine(DeathRoutine());

        gameObject.SetActive(false);
    }
}