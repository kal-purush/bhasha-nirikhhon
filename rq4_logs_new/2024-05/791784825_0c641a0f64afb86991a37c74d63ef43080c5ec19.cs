using UnityEngine;

public class AcidFloor : MonoBehaviour
{
    public float damagePerSecond = 10f; // Amount of damage per second

    private void OnTriggerStay(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            // Apply damage to the player continuously while they are on the acid floor
            PlayerStats playerStats = other.GetComponent<PlayerStats>();
            if (playerStats != null)
            {
                playerStats.TakeDamage(damagePerSecond * Time.deltaTime);
            }
            else
            {
                Debug.LogWarning("PlayerStats component not found on player GameObject.");
            }
        }
    }
}