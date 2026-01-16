using UnityEngine;

public class RockDamage : MonoBehaviour
{
    public int damage = 10; // Amount of damage the rock deals

    private void OnCollisionEnter(Collision collision)
    {
        // Check if the collision is with the player
        PlatformerCharaterController playerController = collision.gameObject.GetComponent<PlatformerCharaterController>();
        if (playerController != null)
        {
            // Deal damage to the player
            playerController.TakeDamage(damage);

            // Destroy the rock after it hits the player
            Destroy(gameObject);
        }
    }
}