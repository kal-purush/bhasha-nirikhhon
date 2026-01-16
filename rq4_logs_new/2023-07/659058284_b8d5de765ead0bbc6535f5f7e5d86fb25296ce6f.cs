using UnityEngine;

public class CheckPointTrigger : MonoBehaviour
{
    [SerializeField] private Transform respawnPosition; // The position where the player should respawn

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player"))
        {
            PlayerCheckPointSystem playerCheckPointSystem = collision.GetComponent<PlayerCheckPointSystem>();
            if (playerCheckPointSystem != null)
            {
                playerCheckPointSystem.SetCheckpoint(respawnPosition.position);
            }
        }
    }
}

