using UnityEngine;

public class Interactable : MonoBehaviour
{
    float interactionRadius = 3f;

    bool isFocus = false;

    bool hasInteracted = false;

    GameObject player;

    public virtual void Interact () {
        Debug.Log("interacting");
    }

    private void Update() {
        if (Input.GetKeyDown(KeyCode.C)){
            if (isFocus && !hasInteracted) {
                Interact();
                hasInteracted = true;
            }
        }
    }

    public bool TryFocusOn (float distance) {
        if (distance <= interactionRadius) {
            return true;
        }

        return false;
    }

    public void OnFocus (GameObject playerObj) {
        isFocus = true;
        player = playerObj;
    }

    public void OnDefocus () {
        isFocus = false;
        hasInteracted = false;
        player = null;
    }
    
    //Draws the radius on dev side (Debug)
    void OnDrawGizmosSelected() {
        Gizmos.color = Color.yellow;
        Gizmos.DrawWireSphere(transform.position, interactionRadius);
    }
}