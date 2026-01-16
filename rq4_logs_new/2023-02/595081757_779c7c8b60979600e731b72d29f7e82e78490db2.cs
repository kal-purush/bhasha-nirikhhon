using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

namespace Interaction
{
    public class HumanInteraction : MonoBehaviour, IInteraction
    {
        private IInteractable heldInteractable;

        // ReSharper disable once ArrangeObjectCreationWhenTypeEvident
        private List<IInteractable> possibleInteractables = new List<IInteractable>();

        private void OnEnable()
        {
            Game.CharacterHandler.OnHumanInteract.AddListener(OnInteract);
        }

        private void OnDisable()
        {
            Game.CharacterHandler.OnHumanInteract.RemoveListener(OnInteract);
        }

        public Transform GetTransform()
        {
            return transform;
        }

        public void AddPossibleInteractable(Interactable interactable)
        {
            if (!possibleInteractables.Contains(interactable))
            {
                possibleInteractables.Add(interactable);
            }
        }

        public void RemovePossibleInteractable(Interactable interactable)
        {
            if (possibleInteractables.Contains(interactable))
            {
                possibleInteractables.Remove(interactable);
            }
        }

        public void OnInteract()
        {
            if (heldInteractable != null)
            {
                StopInteract();
                return;
            }

            InteractClosestInteractable();
        }

        private void InteractClosestInteractable()
        {
            if (possibleInteractables.Count <= 0)
            {
                return;
            }

            var closestInteractable = possibleInteractables[0];
            var closestDistance = float.MaxValue;
            foreach (var interactable in possibleInteractables)
            {
                if (closestInteractable == null)
                {
                    closestInteractable = interactable;
                }

                var currentDistance = Vector3.Distance(transform.position, interactable.GetTransform().position);

                if (!(currentDistance < closestDistance))
                {
                    continue;
                }

                closestInteractable = interactable;
                closestDistance = currentDistance;
            }

            heldInteractable = closestInteractable;
            closestInteractable.Interact(this);
        }

        public void StopInteract()
        {
            heldInteractable.StopInteract();
            heldInteractable = null;
        }
    }
}