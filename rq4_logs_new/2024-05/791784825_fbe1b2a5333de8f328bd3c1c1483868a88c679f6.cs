using UnityEngine;
using System.Collections;

public class AcidFloor : MonoBehaviour
{
    public float damagePerSecond = 10f; // The amount of damage to apply each second
    private PlayerStats playerStats;
    private bool isPlayerOnAcid = false;

    private void OnTriggerEnter(Collider other)
    {
        Debug.Log("OnTriggerEnter called with: " + other.name);
        if (other.CompareTag("Player"))
        {
            Debug.Log("Player entered acid floor trigger.");
            playerStats = other.GetComponent<PlayerStats>();
            if (playerStats != null)
            {
                Debug.Log("PlayerStats component found on player.");
                isPlayerOnAcid = true;
                StartCoroutine(DamagePlayer());
            }
            else
            {
                Debug.Log("PlayerStats component not found on player.");
            }
        }
    }

    private void OnTriggerExit(Collider other)
    {
        Debug.Log("OnTriggerExit called with: " + other.name);
        if (other.CompareTag("Player"))
        {
            Debug.Log("Player exited acid floor trigger.");
            if (playerStats != null)
            {
                isPlayerOnAcid = false;
                StopCoroutine(DamagePlayer());
                playerStats = null;
            }
        }
    }

    private IEnumerator DamagePlayer()
    {
        while (isPlayerOnAcid)
        {
            if (playerStats != null)
            {
                Debug.Log("Applying damage to player.");
                playerStats.TakeDamage(damagePerSecond * Time.deltaTime);
            }
            yield return null; // Wait for the next frame
        }
    }
}