using UnityEngine;

public class HealingStone : MonoBehaviour
{
    [SerializeField] private float healAmount = 1.0f;

    void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player"))
        {
            PlayerHealth health = collision.GetComponent<PlayerHealth>();
            if (health != null)
            {
                health.Heal(healAmount);
                // Optional: play sound or animation
                Destroy(gameObject); // remove the stone
            }
        }
    }
}