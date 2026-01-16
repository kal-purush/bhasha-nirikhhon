using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class InventorySystem : MonoBehaviour
{
    [System.Serializable]
    public class InventorySlot
    {
        public GameObject itemInSlot;
        public RectTransform slotTransform;
        public bool isOccupied;
    }

    [Header("Inventory Settings")]
    [SerializeField] private int maxSlots = 4;
    [SerializeField] private GameObject mapDevicePrefab;
    [SerializeField] private Transform itemHoldPosition;
    [SerializeField] private Color highlightedSlotColor = new Color(1f, 0.8f, 0f, 1f);
    [SerializeField] private Vector3 highlightedScale = new Vector3(1.1f, 1.1f, 1.1f);

    [Header("UI References")]
    [SerializeField] private RectTransform[] slotTransforms;
    [SerializeField] private GameObject slotHighlight;

    [Header("Input")]
    [SerializeField] private InputActionReference slot1Action;
    [SerializeField] private InputActionReference slot2Action;
    [SerializeField] private InputActionReference slot3Action;
    [SerializeField] private InputActionReference slot4Action;
    [SerializeField] private InputActionReference interactAction;

    private InventorySlot[] inventorySlots;
    private int currentActiveSlot = 0;
    private GameObject currentEquippedItem;
    private List<GameObject> itemsInRange = new List<GameObject>();

    private void Awake()
    {
        // Initialize inventory slots
        inventorySlots = new InventorySlot[maxSlots];
        for (int i = 0; i < maxSlots; i++)
        {
            inventorySlots[i] = new InventorySlot
            {
                itemInSlot = null,
                slotTransform = (i < slotTransforms.Length) ? slotTransforms[i] : null,
                isOccupied = false
            };
        }

        // Add map device to first slot
        if (mapDevicePrefab != null)
        {
            AddItemToInventory(mapDevicePrefab);
        }
    }

    private void OnEnable()
    {
        // Enable input actions
        EnableInputActions(true);
    }

    private void OnDisable()
    {
        // Disable input actions
        EnableInputActions(false);
    }

    private void EnableInputActions(bool enable)
    {
        InputActionReference[] actions = { slot1Action, slot2Action, slot3Action, slot4Action, interactAction };

        foreach (var action in actions)
        {
            if (action != null)
            {
                if (enable)
                {
                    action.action.Enable();
                    if (action == slot1Action) action.action.performed += OnSlot1;
                    else if (action == slot2Action) action.action.performed += OnSlot2;
                    else if (action == slot3Action) action.action.performed += OnSlot3;
                    else if (action == slot4Action) action.action.performed += OnSlot4;
                    else if (action == interactAction) action.action.performed += OnInteract;
                }
                else
                {
                    if (action == slot1Action) action.action.performed -= OnSlot1;
                    else if (action == slot2Action) action.action.performed -= OnSlot2;
                    else if (action == slot3Action) action.action.performed -= OnSlot3;
                    else if (action == slot4Action) action.action.performed -= OnSlot4;
                    else if (action == interactAction) action.action.performed -= OnInteract;
                    action.action.Disable();
                }
            }
        }
    }

    private void OnSlot1(InputAction.CallbackContext context) { EquipSlot(0); }
    private void OnSlot2(InputAction.CallbackContext context) { EquipSlot(1); }
    private void OnSlot3(InputAction.CallbackContext context) { EquipSlot(2); }
    private void OnSlot4(InputAction.CallbackContext context) { EquipSlot(3); }

    private void EquipSlot(int slotIndex)
    {
        // Check if slot is in valid range
        if (slotIndex < 0 || slotIndex >= maxSlots)
            return;

        // Check if the slot has an item
        if (!inventorySlots[slotIndex].isOccupied)
            return;

        // Unequip current item if any
        if (currentEquippedItem != null)
        {
            currentEquippedItem.SetActive(false);
        }

        // Equip new item
        currentActiveSlot = slotIndex;
        currentEquippedItem = inventorySlots[slotIndex].itemInSlot;

        if (currentEquippedItem != null)
        {
            currentEquippedItem.SetActive(true);

            // Position in hand/hold position
            currentEquippedItem.transform.position = itemHoldPosition.position;
            currentEquippedItem.transform.rotation = itemHoldPosition.rotation;
            currentEquippedItem.transform.parent = itemHoldPosition;
        }

        // Update UI to show which slot is active
        UpdateSlotHighlight();
    }

    private void UpdateSlotHighlight()
    {
        // Reset all slots to normal appearance
        for (int i = 0; i < slotTransforms.Length; i++)
        {
            if (slotTransforms[i] != null)
            {
                slotTransforms[i].localScale = Vector3.one;
                // If you have an Image component on slots, you can change color here
            }
        }

        // Highlight active slot
        if (currentActiveSlot < slotTransforms.Length && slotTransforms[currentActiveSlot] != null)
        {
            slotTransforms[currentActiveSlot].localScale = highlightedScale;

            // Move highlight visual element if available
            if (slotHighlight != null)
            {
                slotHighlight.transform.position = slotTransforms[currentActiveSlot].position;
                slotHighlight.SetActive(true);
            }
        }
    }

    private void OnInteract(InputAction.CallbackContext context)
    {
        // Check for nearby collectible items
        CheckForItemsInRange();

        if (itemsInRange.Count > 0)
        {
            // Get the closest item
            GameObject closestItem = GetClosestItem();

            if (closestItem != null)
            {
                ScannableObjectScript scannableObject = closestItem.GetComponent<ScannableObjectScript>();

                if (scannableObject != null && scannableObject.IsCollectible())
                {
                    // Add to inventory
                    bool added = AddItemToInventory(closestItem);

                    if (added)
                    {
                        // Collect the item (this will hide it from the world)
                        scannableObject.OnCollect();

                        // Remove from the in-range list
                        itemsInRange.Remove(closestItem);
                    }
                }
            }
        }
    }

    private bool AddItemToInventory(GameObject item)
    {
        // Find first empty slot
        for (int i = 0; i < maxSlots; i++)
        {
            if (!inventorySlots[i].isOccupied)
            {
                // If this is a prefab, instantiate it
                GameObject itemToAdd;
                if (item.scene.name == null)
                {
                    itemToAdd = Instantiate(item);
                }
                else
                {
                    itemToAdd = item;
                }

                // Add to slot
                inventorySlots[i].itemInSlot = itemToAdd;
                inventorySlots[i].isOccupied = true;

                // Initially deactivate the item
                itemToAdd.SetActive(false);

                // Create a UI representation in the slot
                CreateItemUIRepresentation(itemToAdd, inventorySlots[i].slotTransform);

                // If this is the first item added and nothing is equipped, equip it
                if (currentEquippedItem == null && i == 0)
                {
                    EquipSlot(0);
                }

                return true;
            }
        }

        // No empty slots found
        Debug.Log("Inventory is full!");
        return false;
    }

    private void CreateItemUIRepresentation(GameObject item, RectTransform slotTransform)
    {
        if (slotTransform == null) return;

        // Create a simple UI representation (modify as needed for your game)
        GameObject uiRepresentation = new GameObject(item.name + "_UI");
        uiRepresentation.transform.SetParent(slotTransform, false);

        // Add a UI image component
        UnityEngine.UI.Image image = uiRepresentation.AddComponent<UnityEngine.UI.Image>();

        // You might want to use an icon from the item itself if available
        // For now, we'll just use a simple colored image

        // Try to get a distinctive color
        Renderer renderer = item.GetComponentInChildren<Renderer>();
        if (renderer != null && renderer.sharedMaterial != null)
        {
            // Try to get the color from the material
            if (renderer.sharedMaterial.HasProperty("_Color"))
            {
                image.color = renderer.sharedMaterial.color;
            }
            else
            {
                // Default color
                image.color = new Color(0.8f, 0.8f, 0.8f, 1f);
            }
        }
        else
        {
            // Default color
            image.color = new Color(0.8f, 0.8f, 0.8f, 1f);
        }

        // Size the image to fit in the slot
        RectTransform rectTransform = image.rectTransform;
        rectTransform.anchorMin = new Vector2(0.1f, 0.1f);
        rectTransform.anchorMax = new Vector2(0.9f, 0.9f);
        rectTransform.offsetMin = Vector2.zero;
        rectTransform.offsetMax = Vector2.zero;
    }

    private void CheckForItemsInRange()
    {
        // Clear previous list
        itemsInRange.Clear();

        // Find all scannable objects in range
        Collider[] hitColliders = Physics.OverlapSphere(transform.position, 2f, LayerMask.GetMask("Scannable"));

        foreach (var hitCollider in hitColliders)
        {
            ScannableObjectScript scannableObject = hitCollider.GetComponent<ScannableObjectScript>();

            if (scannableObject != null && scannableObject.IsCollectible())
            {
                itemsInRange.Add(hitCollider.gameObject);
            }
        }
    }

    private GameObject GetClosestItem()
    {
        if (itemsInRange.Count == 0) return null;

        GameObject closest = null;
        float closestDistance = float.MaxValue;

        foreach (GameObject item in itemsInRange)
        {
            float distance = Vector3.Distance(transform.position, item.transform.position);

            if (distance < closestDistance)
            {
                closest = item;
                closestDistance = distance;
            }
        }

        return closest;
    }
}