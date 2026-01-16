using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class InteractionHubTeleporter : MonoBehaviour
{
private void Start()
    {
        // Find the object with InteractionSystem component
        InteractionSystem interactionSystem = FindObjectOfType<InteractionSystem>();

        // Subscribe to the OnInteract event
        interactionSystem.OnInteract1 += HandleInteract;
    }

    // Method to handle interaction
    private void HandleInteract(GameObject interactedObject)
    {
        // Check if the interacted object is the one we want to apply selectEffect = 1
        if (interactedObject == gameObject) // Assuming this script is attached to the same GameObject
        {
            // Perform the action equivalent to selectEffect = 1
            interactedObject.GetComponent<ReturnJump>().enabled = true;
        }
    }
}