using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PartsManager : MonoBehaviour
{
    private Camera mainCamera;
    private Transform selectedPart;
    private Vector3 mouseOffset;
    private float mouseZCoord;
    private Vector3 initialLocalPosition;
    private Dictionary<Transform, bool> partMoved = new Dictionary<Transform, bool>();
    private Dictionary<Transform, Vector3> initialPositionsRelativeToTorso = new Dictionary<Transform, Vector3>();

    void Awake()
    {
        mainCamera = Camera.main;
        InitializePartsRelativePosition();
    }

    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            TrySelectPart();
        }

        if (Input.GetMouseButton(0) && selectedPart != null)
        {
            DragSelectedPart();
        }

        if (Input.GetMouseButtonUp(0))
        {
            if (selectedPart != null && partMoved.ContainsKey(selectedPart))
            {
                if (partMoved[selectedPart])
                {
                    // Snap back part only if it was previously marked as moved
                    SnapBackPart();
                    partMoved[selectedPart] = false;
                }
                
                else
                {
                    // Mark part as moved for next click, except for the torso
                    if (!selectedPart.CompareTag("Torso"))
                    {
                        partMoved[selectedPart] = true;
                    }
                }
                selectedPart = null; // Clear selection to prevent unintended dragging
            }
        }
    }

    private void InitializePartsRelativePosition()
    {
        // Initialize relative positions for all parts
        foreach (Transform part in GetComponentsInChildren<Transform>())
        {
            if (part.CompareTag("Torso") || part.CompareTag("Arm"))
            {
                initialPositionsRelativeToTorso[part] = part.localPosition;
                partMoved[part] = false;
            }
        }
    }

    private void TrySelectPart()
    {
        Ray ray = mainCamera.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;

        if (Physics.Raycast(ray, out hit))
        {
            if (hit.collider.CompareTag("Torso") || hit.collider.CompareTag("Arm"))
            {
                selectedPart = hit.transform;
                initialLocalPosition = selectedPart.localPosition;
                mouseZCoord = mainCamera.WorldToScreenPoint(selectedPart.position).z;
                mouseOffset = selectedPart.position - GetMouseWorldPos();
            }
        }
    }

    private void DragSelectedPart()
    {
        if (selectedPart != null)
        {
            Vector3 newWorldPosition = GetMouseWorldPos() + mouseOffset;
            selectedPart.position = newWorldPosition;
        }
    }

    private void SnapBackPart()
    {
        if (selectedPart != null && initialPositionsRelativeToTorso.ContainsKey(selectedPart))
        {
            selectedPart.localPosition = initialPositionsRelativeToTorso[selectedPart];
        }
    }

    private Vector3 GetMouseWorldPos()
    {
        Vector3 mousePoint = Input.mousePosition;
        mousePoint.z = mouseZCoord;
        return mainCamera.ScreenToWorldPoint(mousePoint);
    }
}