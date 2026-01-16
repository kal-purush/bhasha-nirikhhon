using System.Collections;
using Interaction;
using UnityEngine;
using UnityEngine.Serialization;

namespace Puzzles.PushPull
{
    public class Shelf : MonoBehaviour
    {
        public ShelfSlot currentSlot;
        [SerializeField] private float animationDuration;
        [SerializeField] private ShelfInteractable interactableRight;
        [SerializeField] private ShelfInteractable interactableLeft;

        public Transform GetTransform()
        {
            return transform;
        }

        public void StartLerpPosition(ShelfInteractable shelfInteractable)
        {
            StartCoroutine(LerpPosition(shelfInteractable));
        }

        private IEnumerator LerpPosition(ShelfInteractable shelfInteractable)
        {
            var currentTime = 0f;
            var currentPosition = transform.position;
            var targetPosition = currentSlot.transform.position;
            while (currentTime < animationDuration)
            {
                currentTime += Time.deltaTime;
                var newPosition = Vector3.Lerp(currentPosition, targetPosition, currentTime / animationDuration);
                transform.position = newPosition;
                yield return null;
            }

            shelfInteractable.StopInteract();
        }

        public void EnableInteraction()
        {
            interactableLeft.EnableInteractable();
            interactableRight.EnableInteractable();
        }
        
        public void DisableInteraction()
        {
            interactableLeft.DisableInteractable();
            interactableRight.DisableInteractable();
        }
    }
}