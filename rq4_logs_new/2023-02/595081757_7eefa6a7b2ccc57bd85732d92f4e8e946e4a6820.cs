using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

public class HumanInteraction : MonoBehaviour, IPickupInteraction
{
    private Interactable heldInteractable;

    // ReSharper disable once ArrangeObjectCreationWhenTypeEvident
    private List<Interactable> possibleInteractables = new List<Interactable>();

    private void OnEnable()
    {
        Game.CharacterHandler.OnHumanInteract.AddListener(OnInteract());
    }

    private void OnDisable()
    {
        Game.CharacterHandler.OnHumanInteract.RemoveListener(OnInteract());
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

    public void Interact()
    {
        if (heldInteractable != null)
        {
            StopInteract();
            return;
        }

        PickUpClosestInteractable();
    }

    private void PickUpClosestInteractable()
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

            var currentDistance = Vector3.Distance(transform.position, interactable.transform.position);

            if (!(currentDistance < closestDistance))
            {
                continue;
            }

            closestInteractable = interactable;
            closestDistance = currentDistance;
        }

        heldInteractable = closestInteractable;
        closestInteractable.PickUp(gameObject.transform);
    }

    public void StopInteract()
    {
        heldInteractable.Drop();
        heldInteractable = null;
    }

    private UnityAction OnInteract()
    {
        return Interact;
    }
}