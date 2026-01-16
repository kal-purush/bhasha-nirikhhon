using Audio;
using GameConstants;
using UnityEngine;

namespace Puzzle
{
    public class PlaceItemTrigger : MonoBehaviour
    {
        [SerializeField] private RitualManager ritMan;
        [SerializeField] private RitualInventory ritInv;
        
        [SerializeField] private int itemIndex;
        [SerializeField] private GameObject ritualItemInWorld;
        
        [SerializeField] private GameObject interactSprite;
        private bool _canActivate;
        
        private void Start()
        {
            if (interactSprite.activeSelf)
            {
                interactSprite.SetActive(false);
            }
        }

        private void OnEnable()
        {
            Game.Input.OnHumanInteract.AddListener(PlaceItem);
        }

        private void OnDisable()
        {
            Game.Input.OnHumanInteract.RemoveListener(PlaceItem);
        }

        private void OnTriggerEnter(Collider other)
        {
            if (!other.CompareTag(Tags.PlayerTag))
            {
                return;
            }

            _canActivate = true;
            ToggleInteractableUI();
        }

        private void OnTriggerExit(Collider other)
        {
            if (!other.CompareTag(Tags.PlayerTag))
            {
                return;
            }

            _canActivate = false;
            ToggleInteractableUI();
        }

        private void PlaceItem()
        {
            if (!_canActivate)
            {
                return;
            }

            if (!ritInv.CheckForItem(itemIndex))
            {
                return;
            }
            
            DialogueCanvas.Instance.LockRitualItem(itemIndex);
            ritualItemInWorld.SetActive(true);
            ritMan.PlaceItem(itemIndex);
            Destroy(gameObject);
        }

        private void ToggleInteractableUI()
        {
            interactSprite.SetActive(!interactSprite.activeSelf);
        }
    }
}