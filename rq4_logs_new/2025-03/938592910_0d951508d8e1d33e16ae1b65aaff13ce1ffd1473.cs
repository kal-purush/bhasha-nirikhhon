using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class CollectibleItem : MonoBehaviour
{
    [Header("Item Settings")]
    [SerializeField] private string itemName = "Item";
    [SerializeField] private string itemDescription = "A collectible item";
    [SerializeField] private bool isAutoCollect = false; // If true, collect on any collision
    [SerializeField] private float interactionDistance = 3f; // How close player needs to be

    [Header("UI Prompt")]
    [SerializeField] private GameObject interactionPromptPrefab; // Assign a prefab with TextMeshPro
    [SerializeField] private Vector3 promptOffset = new Vector3(0, 1.5f, 0); // Position above item
    [SerializeField] private string promptText = "Press E to collect";

    // References
    private GameObject promptInstance;
    private TextMeshProUGUI promptTextComponent;
    private Camera mainCamera;
    private ScannableObjectScript scannableScript;

    // States
    private bool isInRange = false;
    private bool isLookingAt = false;
    private Transform player;
    private InventorySystem playerInventory;

    private void Awake()
    {
        // Find the main camera
        mainCamera = Camera.main;

        // Get or add ScannableObjectScript component (required for radar detection)
        scannableScript = GetComponent<ScannableObjectScript>();
        if (scannableScript == null)
        {
            scannableScript = gameObject.AddComponent<ScannableObjectScript>();
        }

        // Set the object as collectible in the ScannableObjectScript
        MarkAsCollectible();
    }

    private void Start()
    {
        // Create the interaction prompt if prefab is assigned
        if (interactionPromptPrefab != null)
        {
            promptInstance = Instantiate(interactionPromptPrefab, transform.position + promptOffset, Quaternion.identity);
            promptInstance.transform.parent = transform;

            // Find the TextMeshPro component
            promptTextComponent = promptInstance.GetComponentInChildren<TextMeshProUGUI>();
            if (promptTextComponent != null)
            {
                promptTextComponent.text = promptText;
            }

            // Hide the prompt initially
            promptInstance.SetActive(false);
        }

        // Set the layer to Scannable to ensure it works with the radar
        gameObject.layer = LayerMask.NameToLayer("Scannable");
    }

    private void Update()
    {
        // Check if player is nearby
        CheckPlayerInRange();

        // Check if player is looking at this object
        CheckPlayerLookingAt();

        // Update prompt visibility
        UpdatePrompt();

        // Check for input to collect
        if (isInRange && isLookingAt && Input.GetKeyDown(KeyCode.E))
        {
            AttemptCollect();
        }
    }

    private void CheckPlayerInRange()
    {
        // First try to find player if we don't have it yet
        if (player == null)
        {
            GameObject playerObj = GameObject.FindGameObjectWithTag("Player");
            if (playerObj != null)
            {
                player = playerObj.transform;
                playerInventory = playerObj.GetComponent<InventorySystem>();
                if (playerInventory == null)
                {
                    playerInventory = playerObj.GetComponentInChildren<InventorySystem>();
                }
            }
        }

        if (player != null)
        {
            float distance = Vector3.Distance(transform.position, player.position);
            isInRange = distance <= interactionDistance;
        }
    }

    private void CheckPlayerLookingAt()
    {
        if (mainCamera == null || !isInRange)
        {
            isLookingAt = false;
            return;
        }

        // Cast a ray from camera to check if it hits this object
        Ray ray = mainCamera.ScreenPointToRay(new Vector3(Screen.width / 2, Screen.height / 2, 0));
        RaycastHit hit;

        if (Physics.Raycast(ray, out hit, interactionDistance))
        {
            isLookingAt = hit.transform == transform || hit.transform.IsChildOf(transform);
        }
        else
        {
            isLookingAt = false;
        }
    }

    private void UpdatePrompt()
    {
        if (promptInstance != null)
        {
            bool shouldShowPrompt = isInRange && isLookingAt;

            // Only update if needed
            if (promptInstance.activeSelf != shouldShowPrompt)
            {
                promptInstance.SetActive(shouldShowPrompt);
            }

            // If showing, make sure it faces the camera
            if (shouldShowPrompt && mainCamera != null)
            {
                promptInstance.transform.LookAt(2 * promptInstance.transform.position - mainCamera.transform.position);
            }
        }
    }

    private void AttemptCollect()
    {
        if (playerInventory != null)
        {
            bool collected = playerInventory.AddItem(gameObject);

            if (collected)
            {
                // If successfully added to inventory, notify the scannable script
                if (scannableScript != null)
                {
                    scannableScript.OnCollect();
                }
                else
                {
                    // Fallback if no scannable script
                    gameObject.SetActive(false);
                }
            }
        }
    }

    private void MarkAsCollectible()
    {
        // Use reflection to set the isCollectible field if it exists in ScannableObjectScript
        // This is a workaround if your ScannableObjectScript doesn't expose IsCollectible setter
        var field = typeof(ScannableObjectScript).GetField("isCollectible",
                                                           System.Reflection.BindingFlags.Instance |
                                                           System.Reflection.BindingFlags.NonPublic);
        if (field != null)
        {
            field.SetValue(scannableScript, true);
        }
    }

    // Automatically collect when player walks into the item if isAutoCollect is true
    private void OnTriggerEnter(Collider other)
    {
        if (isAutoCollect && other.CompareTag("Player"))
        {
            if (playerInventory == null)
            {
                playerInventory = other.GetComponent<InventorySystem>();
                if (playerInventory == null)
                {
                    playerInventory = other.GetComponentInChildren<InventorySystem>();
                }
            }

            if (playerInventory != null)
            {
                AttemptCollect();
            }
        }
    }

    // Public methods to get item information
    public string GetItemName()
    {
        return itemName;
    }

    public string GetItemDescription()
    {
        return itemDescription;
    }
}