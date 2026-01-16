using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.Events;

namespace Puzzles.PushPull
{
    public class PushPullPuzzle : MonoBehaviour
    {
        public UnityEvent OnSolved;

        [SerializeField] private List<GameObject> shelfGameObjects = new List<GameObject>();
        [SerializeField] private List<GameObject> shelfSlotsObjects = new List<GameObject>();

        private readonly List<ShelfSlot> correctSlots = new List<ShelfSlot>();
        private readonly List<ShelfSlot> shelfSlots = new List<ShelfSlot>();

        private void Awake()
        {
            // Set Slots Indexes
            var currentIndex = 0;
            foreach (var shelfSlotObject in shelfSlotsObjects)
            {
                shelfSlots.Add(shelfSlotsObjects[currentIndex].GetComponent<ShelfSlot>());
                shelfSlotObject.GetComponent<ShelfSlot>().slotIndex = currentIndex;
                currentIndex++;
            }


            // Assign shelves to their slots
            shelfSlots[2].ShelfInteractable = shelfGameObjects[0].GetComponent<ShelfInteractable>();
            shelfSlots[4].ShelfInteractable = shelfGameObjects[1].GetComponent<ShelfInteractable>();
            shelfSlots[6].ShelfInteractable = shelfGameObjects[2].GetComponent<ShelfInteractable>();
            shelfSlots[8].ShelfInteractable = shelfGameObjects[3].GetComponent<ShelfInteractable>();

            // Assign shelves current slots references
            shelfSlots[2].ShelfInteractable.currentSlot = shelfSlots[2];
            shelfSlots[4].ShelfInteractable.currentSlot = shelfSlots[4];
            shelfSlots[6].ShelfInteractable.currentSlot = shelfSlots[6];
            shelfSlots[8].ShelfInteractable.currentSlot = shelfSlots[8];

            // Set correct slots
            correctSlots.Add(shelfSlots[0]);
            correctSlots.Add(shelfSlots[2]);
            correctSlots.Add(shelfSlots[7]);
            correctSlots.Add(shelfSlots[9]);
        }

        private void Start()
        {
            foreach (var shelfGameObject in shelfGameObjects)
            {
                if (!shelfGameObject.TryGetComponent<ShelfInteractable>(out var shelfInteractable))
                {
                    continue;
                }

                shelfInteractable.EnableInteractable();
            }
        }

        public bool CanMoveShelf(ShelfInteractable movingShelfInteractable, bool isDirectionRight)
        {
            switch (isDirectionRight)
            {
                case true:
                    if (movingShelfInteractable.currentSlot.slotIndex + 1 > shelfSlots.Count)
                    {
                        return false;
                    }

                    return shelfSlots[movingShelfInteractable.currentSlot.slotIndex + 1].ShelfInteractable == null;
                case false:
                    if (movingShelfInteractable.currentSlot.slotIndex - 1 < 0)
                    {
                        return false;
                    }

                    return shelfSlots[movingShelfInteractable.currentSlot.slotIndex - 1].ShelfInteractable == null;
            }
        }

        public void CheckSolved()
        {
            if (correctSlots.Any(correctSlot => correctSlot.ShelfInteractable == null))
            {
                return;
            }

            foreach (var shelfGameObject in shelfGameObjects)
            {
                shelfGameObject.GetComponent<ShelfInteractable>().DisableInteraction();
            }

            Debug.Log("Puzzle solved!");
            OnSolved.Invoke();
            LockPuzzle();
        }

        private void LockPuzzle()
        {
            foreach (var shelfSlot in shelfSlots.Where(shelfSlot => shelfSlot.ShelfInteractable != null))
            {
                shelfSlot.ShelfInteractable.DisableInteraction();
            }
        }

        public void SetNewShelfPosition(ShelfInteractable movingShelfInteractable, bool movingRight)
        {
            if (movingRight)
            {
                // Move shelf one slot to the right
                Debug.Log("Moving shelf right");
                shelfSlots[movingShelfInteractable.currentSlot.slotIndex + 1].ShelfInteractable =
                    movingShelfInteractable;
                shelfSlots[movingShelfInteractable.currentSlot.slotIndex].ShelfInteractable = null;

                movingShelfInteractable.currentSlot = shelfSlots[movingShelfInteractable.currentSlot.slotIndex + 1];
                movingShelfInteractable.UpdatePosition();
            }
            else
            {
                // Move shelf one slot to the left
                Debug.Log("Moving shelf left");
                shelfSlots[movingShelfInteractable.currentSlot.slotIndex - 1].ShelfInteractable =
                    movingShelfInteractable;
                shelfSlots[movingShelfInteractable.currentSlot.slotIndex].ShelfInteractable = null;
                movingShelfInteractable.currentSlot = shelfSlots[movingShelfInteractable.currentSlot.slotIndex - 1];
                movingShelfInteractable.UpdatePosition();
            }
        }
    }
}