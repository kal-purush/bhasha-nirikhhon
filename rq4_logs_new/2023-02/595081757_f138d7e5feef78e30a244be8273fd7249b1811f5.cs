using System;
using Events;
using GameConstants;
using UnityEngine;

namespace Interaction
{
    public class InventoryInteractable : MonoBehaviour
    {
        [SerializeField] private PhantomTetherEvent onPickupEvent;
        [SerializeField] private GameObject interactableUI;

        private void OnTriggerEnter(Collider other)
        {
            if (!other.CompareTag(Tags.PlayerTag))
            {
                return;
            }

            interactableUI.SetActive(true);
            Game.Input.OnHumanInteract.AddListener(OnInteract);
        }

        private void OnTriggerExit(Collider other)
        {
            if (!other.CompareTag(Tags.PlayerTag))
            {
                return;
            }
            
            interactableUI.SetActive(false);
            Game.Input.OnHumanInteract.RemoveListener(OnInteract);
        }

        private void OnInteract()
        {
            interactableUI.SetActive(false);
            Game.Input.OnHumanInteract.RemoveListener(OnInteract);
            
            onPickupEvent.RaiseEvent();
        }
    }
}