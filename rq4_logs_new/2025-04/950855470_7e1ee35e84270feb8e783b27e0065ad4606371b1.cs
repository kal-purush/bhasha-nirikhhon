using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI; // Required for Selectable checks

public class NavigationRestorer : MonoBehaviour
{
    private GameObject lastSelectedObject;
    private EventSystem eventSystem;

    void Start()
    {
        // Cache the EventSystem for efficiency
        eventSystem = EventSystem.current;
        if (eventSystem == null)
        {
            Debug.LogError("NavigationRestorer requires an EventSystem in the scene.", this);
            this.enabled = false; // Disable script if no EventSystem
        }
    }

    void Update()
    {
        if (eventSystem == null) return; // Safety check

        GameObject currentSelection = eventSystem.currentSelectedGameObject;

        // If something is currently selected, remember it.
        if (currentSelection != null)
        {
            lastSelectedObject = currentSelection;
        }
        // If nothing is selected...
        else
        {
            // ...and we have a record of the last selected item...
            if (lastSelectedObject != null)
            {
                // ...and the user tries to navigate (using standard axes)...
                // Adapt this input check if using the new Input System.
                float horizontalInput = Input.GetAxisRaw("Horizontal");
                float verticalInput = Input.GetAxisRaw("Vertical");

                if (horizontalInput != 0f || verticalInput != 0f)
                {
                    // Before restoring, check if the last object is still valid
                    // (active, interactable). This prevents errors if the
                    // object was disabled or destroyed.
                    Selectable selectable = lastSelectedObject.GetComponent<Selectable>();
                    if (lastSelectedObject.activeInHierarchy &&
                         selectable != null &&
                         selectable.IsInteractable())
                    {
                        // Restore selection!
                        eventSystem.SetSelectedGameObject(lastSelectedObject);
                        // Debug.Log($"UI Navigation Restored to: {lastSelectedObject.name}");
                    }
                    else
                    {
                        // The last object is no longer valid, so clear our memory.
                        // Otherwise, we might keep trying to select an invalid object.
                        lastSelectedObject = null;
                        // Optionally, you could try finding the *first* available
                        // selectable element here as a fallback, but that's more complex.
                    }
                }
            }
        }
    }

    // Optional: A public method to manually set the last selected object
    // if needed for specific game logic (e.g., when closing a sub-menu).
    public void SetLastSelected(GameObject obj)
    {
        if (obj != null && obj.GetComponent<Selectable>() != null)
        {
            lastSelectedObject = obj;
        }
    }
}