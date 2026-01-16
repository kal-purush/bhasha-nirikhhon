using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class OptimizedComponentManager : MonoBehaviour
{
    public DetachablePartsConfig partsConfig;                     // Reference to the DetachablePartsConfig scriptable object
    public static event Action<bool?> OnComponentStatusChanged;   // Event to notify UI of component status
    public LayerMask componentLayer;                              // Layer mask for component objects
    private Camera mainCamera;                                    // Reference to the main camera
    private Transform selectedPart;                               // Reference to the selected component's transform
    private Vector3 mouseOffset;                                  // Offset between the mouse and the component's position
    private float mouseZCoord;                                    // Z-coordinate of the mouse in world space
    private Vector3 initialLocalPosition;                         // Initial local position of the selected component
    private Vector3 lastMousePosition = Vector3.zero;             // Last mouse position
    private float lastRaycastTime = 0f;                           // Last time a raycast was performed
    private float raycastFrequency = 0.1f;                        // Interval between raycasts


    // Dictionaries to track moved components and their initial positions relative to the torso
    private Dictionary<Transform, bool> partMoved = new Dictionary<Transform, bool>();
    private Dictionary<Transform, Vector3> initialPositionsRelativeToTorso = new Dictionary<Transform, Vector3>();

    
    void Awake()
    {
        mainCamera = Camera.main;               // Initialize the reference to the main camera
        InitializePartsRelativePosition();      // Initialize component's relative positions
    }

    void Update()
    {
        // Process mouse input to select, move, and snap components
        if (Input.GetMouseButtonDown(0))   
        {
            TrySelectPart();      // Attempt to select a component
        }

        if (Input.GetMouseButton(0) && selectedPart != null)   
        {
            DragSelectedPart();   // Drag the component if one is selected
        }

        if (Input.GetMouseButtonUp(0))   
        {
            // Check if component is selected and is found in the dictionary
            if (selectedPart != null && partMoved.ContainsKey(selectedPart))   
            {
                if (partMoved[selectedPart])  
                {
                    // Snap back only if component was previously marked as moved
                    SnapBackPart();
                    partMoved[selectedPart] = false;
                }
                
                else
                {
                    // Mark component as moved for next click, except for the torso
                    if (!selectedPart.CompareTag("Torso"))
                    {
                        partMoved[selectedPart] = true;
                    }
                }
                selectedPart = null; 
            }
        }
    }

    private void InitializePartsRelativePosition()
    {
        // Initialize components' relative positions based on the DetachablePartsConfig
        InitializePartPositions(partsConfig.eyeComponents);
        InitializePartPositions(partsConfig.headComponents);
        InitializePartPositions(partsConfig.torsoComponents);
        InitializePartPositions(partsConfig.rightArmComponents);
        InitializePartPositions(partsConfig.leftArmComponents);
        InitializePartPositions(partsConfig.rightLegComponents);
        InitializePartPositions(partsConfig.leftLegComponents);
    }

    
    private void InitializePartPositions(List<DetachablePartsConfig.PartConfig> partConfigs) 
    {
        // Initialize components' initial positions
        foreach (var partConfig in partConfigs)
        {
            if (partConfig.gameObject != null)
            {
                initialPositionsRelativeToTorso[partConfig.gameObject.transform] = partConfig.gameObject.transform.localPosition;
                partMoved[partConfig.gameObject.transform] = false;
            }
        }
    }

    
    private void TrySelectPart() 
    {
        // TrySelectPart checks if a partTransform is in any of the part lists

        // Check if the mouse position has changed, if left mouse button has been pressed, and if raycast frequency has been reached
        if (Input.mousePosition != lastMousePosition && Input.GetMouseButtonDown(0) && Time.time - lastRaycastTime >= raycastFrequency) 
        {
            Ray ray = mainCamera.ScreenPointToRay(Input.mousePosition);
            RaycastHit hit;

            if (Physics.Raycast(ray, out hit, Mathf.Infinity, componentLayer))
            {
                Transform hitTransform = hit.transform;
                
                if (IsConfiguredPart(hitTransform))
                {
                    // Select the component and store its initial position
                    selectedPart = hitTransform;
                    initialLocalPosition = selectedPart.localPosition;
                    mouseZCoord = mainCamera.WorldToScreenPoint(selectedPart.position).z;
                    mouseOffset = selectedPart.position - GetMouseWorldPos();

                    OnComponentStatusChanged?.Invoke(hit.collider.CompareTag("Torso") ? null : false); 
                }
            }

            // Update the last mouse position and raycast time
            lastMousePosition = Input.mousePosition;   
            lastRaycastTime = Time.time;   
        }

    }


    private bool IsConfiguredPart(Transform partTransform)
    {
        // Check if the partTransform is in any of the part lists - if so, return true
        return partsConfig.IsAnyPart(partTransform);
    }

    private void DragSelectedPart()
    {
        // Drag the selected component to the new position
        if (selectedPart != null)
        {
            Vector3 newWorldPosition = GetMouseWorldPos() + mouseOffset;
            selectedPart.position = newWorldPosition;

            if (!selectedPart.CompareTag("Torso"))
            {
                OnComponentStatusChanged?.Invoke(false);
            }
        }
    }

    private void SnapBackPart()
    {
        // Snap the selected component back to initial position using the local position
        if (selectedPart != null && initialPositionsRelativeToTorso.ContainsKey(selectedPart))
        {
            OnComponentStatusChanged?.Invoke(true);
            selectedPart.localPosition = initialPositionsRelativeToTorso[selectedPart];
        }
    }

    private Vector3 GetMouseWorldPos()
    {
        // Get the mouse position in world coordinates
        Vector3 mousePoint = Input.mousePosition;
        mousePoint.z = mouseZCoord;
        return mainCamera.ScreenToWorldPoint(mousePoint);
    }

    public void UpdateComponentStatus(bool? isDetached)
    {
        // Update UI with component status
        OnComponentStatusChanged?.Invoke(isDetached);
    }
}