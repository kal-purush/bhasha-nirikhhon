using UnityEngine;
using UnityEngine.UI;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.InputSystem;

public class MapDevice : MonoBehaviour
{
    [Header("Scanner Settings")]
    [SerializeField] private float scanRadius = 5f;
    [SerializeField] private float scanFrequency = 0.5f; // How often to scan (in seconds)
    [SerializeField] private float hologramShowAngle = 30f; // Angle within which to show hologram
    [SerializeField] private LayerMask scannableLayer; // Layer for scannable objects

    [Header("Visual Elements")]
    [SerializeField] private GameObject mapDeviceUI; // The UI panel that contains the radar
    [SerializeField] private RectTransform radarDisplay; // The circular radar display
    [SerializeField] private GameObject dotPrefab; // Red dot prefab to instantiate on radar
    [SerializeField] private GameObject hologramEffectPrefab; // Hologram effect to show on objects
    [SerializeField] private RectTransform radarSweeperImage;

    [Header("Audio")]
    [SerializeField] private AudioClip scanSound;
    [SerializeField] private AudioClip objectFoundSound;
    [SerializeField] private AudioSource audioSource;

    [Header("Input")]
    [SerializeField] private InputActionReference activateScannerAction;

    // Private variables
    private bool isScanning = false;
    private float scanTimer = 0f;
    private List<ScannableObjectScript> detectedObjects = new List<ScannableObjectScript>();
    private Dictionary<ScannableObjectScript, GameObject> radarDots = new Dictionary<ScannableObjectScript, GameObject>();
    private Dictionary<ScannableObjectScript, GameObject> hologramEffects = new Dictionary<ScannableObjectScript, GameObject>();
    private Transform playerCamera;

    // Sweep animation variables;
    private float sweepRoation = 0f;
    private float currentAngle = 0f;
    [SerializeField] private float sweepSpeed = 120f;

    // Cache
    private List<ScannableObjectScript> objectsToRemove = new List<ScannableObjectScript>();

    private void Awake()
    {
        // Find the main camera if not directly assigned
        if (playerCamera == null)
        {
            playerCamera = Camera.main.transform;
        }

        // Set up scanner UI initially hidden
        if (mapDeviceUI != null)
        {
            mapDeviceUI.SetActive(false);
        }

        // Make sure we have an audio source
        if (audioSource == null)
        {
            audioSource = gameObject.AddComponent<AudioSource>();
        }
    }

    private void OnEnable()
    {
        // Enable input action
        if (activateScannerAction != null)
        {
            activateScannerAction.action.Enable();
            activateScannerAction.action.performed += OnToggleScanner;
        }
    }

    private void OnDisable()
    {
        // Disable input action
        if (activateScannerAction != null)
        {
            activateScannerAction.action.performed -= OnToggleScanner;
            activateScannerAction.action.Disable();
        }

        // Clean up hologram effects when disabled
        CleanupHolograms();
    }

    private void OnToggleScanner(InputAction.CallbackContext context)
    {
        isScanning = !isScanning;

        // Toggle the UI
        if (mapDeviceUI != null)
        {
            mapDeviceUI.SetActive(isScanning);
        }

        // Play activation sound
        if (audioSource != null && scanSound != null && isScanning)
        {
            audioSource.PlayOneShot(scanSound);
        }

        // Clear radar when deactivated
        if (!isScanning)
        {
            ClearRadar();
            CleanupHolograms();
        }
    }

    private void Update()
    {
        if (!isScanning) return;

        // Increment scan timer
        scanTimer += Time.deltaTime;

        Sweeper();

        // Perform scan at regular intervals
        if (scanTimer >= scanFrequency)
        {
            PerformScan();
            scanTimer = 0f;
        }

        // Update radar dots positions
        UpdateRadarDisplay();

        // Check which objects are in view to show holograms
        UpdateHolograms();
    }

    private void PerformScan()
    {
        // Find all scannable objects within radius
        Collider[] hitColliders = Physics.OverlapSphere(transform.position, scanRadius, scannableLayer);

        // Play scan sound
        if (audioSource != null && scanSound != null)
        {
            audioSource.PlayOneShot(scanSound, 0.5f);
        }

        // Track which objects we need to remove (no longer in range)
        objectsToRemove.Clear();
        objectsToRemove.AddRange(detectedObjects);

        foreach (var hitCollider in hitColliders)
        {
            // Get the ScannableObject component
            ScannableObjectScript scannableObject = hitCollider.GetComponent<ScannableObjectScript>();

            if (scannableObject != null)
            {
                // Check if this is a new object
                if (!detectedObjects.Contains(scannableObject))
                {
                    // Add new object to detected list
                    detectedObjects.Add(scannableObject);

                    // Create radar dot for this object
                    CreateRadarDot(scannableObject);

                    // Play object found sound
                    if (audioSource != null && objectFoundSound != null)
                    {
                        audioSource.PlayOneShot(objectFoundSound);
                    }
                }
                else
                {
                    // Object was already detected, remove from the to-remove list
                    objectsToRemove.Remove(scannableObject);
                }
            }
        }

        // Remove objects that are no longer in range
        foreach (var obj in objectsToRemove)
        {
            RemoveDetectedObject(obj);
        }
    }

    private void CreateRadarDot(ScannableObjectScript scannableObject)
    {
        if (radarDisplay == null || dotPrefab == null) return;

        // Instantiate dot prefab as child of radar display
        GameObject dot = Instantiate(dotPrefab, radarDisplay);

        // Store reference to the dot
        radarDots[scannableObject] = dot;

        // Notify the scannable object it was detected
        scannableObject.OnDetected();
    }

    private void Sweeper()
    {
        if (radarSweeperImage == null) return;

        currentAngle = (currentAngle + sweepSpeed * Time.deltaTime) % 360;
        radarSweeperImage.localRotation = Quaternion.Euler(0, 0, -currentAngle);

    }

    private void UpdateRadarDisplay()
    {
        if (radarDisplay == null) return;

        // Get radar display dimensions
        float radarRadius = radarDisplay.rect.width * 0.5f;

        Vector3 playerForward = playerCamera.forward;
        playerForward.y = 0;
        playerForward.Normalize();

        float playerAngle = Vector3.SignedAngle(Vector3.forward, playerForward, Vector3.up);

        foreach (var obj in detectedObjects)
        {
            if (obj == null) continue;

            // Get radar dot for this object
            if (radarDots.TryGetValue(obj, out GameObject dot))
            {
                if (dot == null) continue;

                // Calculate relative position from player to object
                Vector3 relativePos = obj.transform.position - transform.position;

                // Calculate 2D position on radar (normalized by scan radius)
                float distance = relativePos.magnitude;
                float normalizedDist = Mathf.Clamp01(distance / scanRadius);

                // Convert to radar space coordinates
                Vector3 flatRelative = new Vector3(relativePos.x, 0, relativePos.z);
                float objectAngle = Vector3.SignedAngle(Vector3.forward, flatRelative, Vector3.up);

                float radarAngle = objectAngle - playerAngle;
                float radarAngleRad = radarAngle * Mathf.Deg2Rad;

                // Calculate position on radar
                float x = Mathf.Sin(radarAngleRad) * normalizedDist * radarRadius;
                float y = Mathf.Cos(radarAngleRad) * normalizedDist * radarRadius;

                // Apply position to dot's RectTransform
                RectTransform dotRect = dot.GetComponent<RectTransform>();
                if (dotRect != null)
                {
                    dotRect.anchoredPosition = new Vector2(x,y);
                }
            }
        }
    }

    private void UpdateHolograms()
    {
        foreach (var obj in detectedObjects)
        {
            if (obj == null) continue;

            // Calculate direction to object
            Vector3 directionToObject = (obj.transform.position - playerCamera.position).normalized;

            // Check if object is in front of the player (dot product with camera forward)
            float dotProduct = Vector3.Dot(playerCamera.forward, directionToObject);

            // Calculate angle between camera forward and direction to object
            float angleToObject = Vector3.Angle(playerCamera.forward, directionToObject);

            // Check if object is within the show angle
            bool shouldShowHologram = (angleToObject <= hologramShowAngle) && (dotProduct > 0);

            // Update hologram visibility
            if (shouldShowHologram)
            {
                ShowHologram(obj);
            }
            else
            {
                HideHologram(obj);
            }
        }
    }

    private void ShowHologram(ScannableObjectScript obj)
    {
        // Check if we already have a hologram for this object
        if (hologramEffects.TryGetValue(obj, out GameObject hologram) && hologram != null)
        {
            // Hologram already exists and is active
            return;
        }

        // Create new hologram effect
        if (hologramEffectPrefab != null)
        {
            GameObject newHologram = Instantiate(hologramEffectPrefab, obj.transform.position, Quaternion.identity);

            // Parent to the scannable object if it has a specific attachment point
            Transform attachPoint = obj.GetHologramAttachPoint();
            if (attachPoint != null)
            {
                newHologram.transform.SetParent(attachPoint);
                newHologram.transform.localPosition = Vector3.zero;
            }

            // Store reference
            hologramEffects[obj] = newHologram;

            // Notify the object that its hologram is shown
            obj.OnHologramDisplayed();
        }
    }

    private void HideHologram(ScannableObjectScript obj)
    {
        // Check if we have a hologram for this object
        if (hologramEffects.TryGetValue(obj, out GameObject hologram) && hologram != null)
        {
            // Hide the hologram
            Destroy(hologram);
            hologramEffects.Remove(obj);

            // Notify the object that its hologram is hidden
            obj.OnHologramHidden();
        }
    }

    private void RemoveDetectedObject(ScannableObjectScript obj)
    {
        detectedObjects.Remove(obj);

        // Remove radar dot
        if (radarDots.TryGetValue(obj, out GameObject dot) && dot != null)
        {
            Destroy(dot);
        }
        radarDots.Remove(obj);

        // Remove hologram if it exists
        HideHologram(obj);

        // Notify the object it's no longer detected
        if (obj != null)
        {
            obj.OnLostDetection();
        }
    }

    private void ClearRadar()
    {
        // Destroy all dots on the radar
        foreach (var dot in radarDots.Values)
        {
            if (dot != null)
            {
                Destroy(dot);
            }
        }

        radarDots.Clear();
        detectedObjects.Clear();
    }

    private void CleanupHolograms()
    {
        // Destroy all active holograms
        foreach (var hologram in hologramEffects.Values)
        {
            if (hologram != null)
            {
                Destroy(hologram);
            }
        }

        hologramEffects.Clear();
    }

    // For debugging
    private void OnDrawGizmosSelected()
    {
        // Draw scan radius
        Gizmos.color = Color.cyan;
        Gizmos.DrawWireSphere(transform.position, scanRadius);

        // Draw hologram view angle
        if (playerCamera != null)
        {
            Gizmos.color = Color.yellow;
            Vector3 forward = playerCamera.forward;
            Vector3 right = Quaternion.Euler(0, hologramShowAngle, 0) * forward;
            Vector3 left = Quaternion.Euler(0, -hologramShowAngle, 0) * forward;

            Gizmos.DrawRay(playerCamera.position, forward * scanRadius);
            Gizmos.DrawRay(playerCamera.position, right * scanRadius);
            Gizmos.DrawRay(playerCamera.position, left * scanRadius);
        }
    }
}