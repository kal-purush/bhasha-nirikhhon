using UnityEngine;

public class  ClickablePathingArrow : MonoBehaviour
{
    public LayerMask clickableLayer;
    public Collider hoverbox; // Make sure this specific collider is assigned in the Inspector
    public MeshRenderer arrow; // Make sure the arrow's MeshRenderer is assigned
    public Material arrowMat; // Assign the default material
    public Material arrowMatHovered; // Assign the hover material

    public int pathIndex; // Reference to the path in navigable paths in Board.cs
    private Board board;

    // No need for Start() if it's empty

    void Update()
    {
        // Ensure required components are assigned to prevent errors
        if (hoverbox == null || arrow == null || arrowMat == null || arrowMatHovered == null || Camera.main == null)
        {
            Debug.LogWarning("HighlightOnHover: Missing references in the Inspector or no Main Camera tagged.", this);
            return; // Stop execution if setup is incomplete
        }

        Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;
        bool isHoveringTarget = false; // Track if we are hovering over the specific target

        // Perform the raycast
        if (Physics.Raycast(ray, out hit, Mathf.Infinity, clickableLayer))
        {
            // Check if the collider we hit is the specific one we care about
            if (hit.collider == hoverbox)
            {
                isHoveringTarget = true;
            }
        }

        // Set the material based on whether we are hovering the target or not
        if (isHoveringTarget)
        {
            if (Input.GetMouseButtonDown(0))
            {
                board.SelectPath(pathIndex);
            }
            if (arrow.material != arrowMatHovered)
            {
                arrow.material = arrowMatHovered;
            }
        }
        else
        {
            if (arrow.material != arrowMat)
            {
                arrow.material = arrowMat;
            }
        }
    }

    public void SetBoard(Board b)
    {
        board = b;
    }
}