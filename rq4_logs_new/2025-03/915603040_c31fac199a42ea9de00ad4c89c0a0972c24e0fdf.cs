using UnityEngine;

public class CampfireInteraction : MonoBehaviour
{
    private bool isPlayerNearby = false;

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag("Player"))
        {
            isPlayerNearby = true;
            Debug.Log("Press E to save game.");
        }
    }

    private void OnTriggerExit2D(Collider2D other)
    {
        if (other.CompareTag("Player"))
        {
            isPlayerNearby = false;
        }
    }

    private void Update()
    {
        if (isPlayerNearby && Input.GetKeyDown(KeyCode.E))
        {
            DBController.Instance.SaveNow();
            Debug.Log("Game Saved at Campfire!");
            // UIManager.Instance.ShowMessage("Game Saved!", 2f);
        }
    }
}