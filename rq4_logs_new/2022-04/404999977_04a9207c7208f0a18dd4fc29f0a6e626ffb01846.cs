using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class SS_GUIAreaController : MonoBehaviour
{
    [Tooltip("The button on the controller that activates the purchase whilst in the trigger/purchase area.")]
    [SerializeField] private List<InputActionReference> purchaseActionRef;

    private void OnTriggerExit(Collider other)
    {
        // Check if the other is the Player, if so invoke the OnPurchaseBegin event.
        if (other.gameObject.tag == "Player") GameManager.instance.OnPurchaseAborted();
    }

    private void OnTriggerEnter(Collider other)
    {
        // Check if the other is the Player, if so show the tooltip for how to activate the purchase menu.
        //if (other.gameObject.tag == "Player") GameManager.instance.OnPurchaseBegin();
    }

    private void OnTriggerStay(Collider other)
    {
        // Check if the other is the Player, if so invoke the OnPurchaseBegin event.
        if (other.gameObject.tag != "Player") return;
            // Check if the purchase is active, if so invoke the OnPurchaseCompleted event.
        foreach (InputActionReference actionRef in purchaseActionRef)
        {
            if (!actionRef.action.triggered) return;
            GameManager.instance.OnPurchaseBegin();
        }
    }

    public void InvokePurchase() => GameManager.instance.OnPurchaseCompleted();
}