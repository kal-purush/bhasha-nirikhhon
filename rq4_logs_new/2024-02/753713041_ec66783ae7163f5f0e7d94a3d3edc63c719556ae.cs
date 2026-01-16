using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HighlightManager : MonoBehaviour
{
    public Material highlightMat;
    public Material armHighlightMat;
    public Material defaultMat;
    public Renderer[] robotRenderers;
    public Renderer[] armRenderers;
    public LayerMask highlightLayer;


   
    private void Update()
    {
        // Check if the mouse is over a robot part
        Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;

        // If the ray hits a collider, check the tag and highlight the corresponding group
        if (Physics.Raycast (ray, out hit, Mathf.Infinity, highlightLayer))
        {
           switch (hit.collider.tag)
           {
              case "Torso": // Highlight the robot
                HighlightGroup(robotRenderers, highlightMat);
                HighlightGroup(armRenderers, highlightMat);
                break;

              case "Arm": // Highlight the arm
                HighlightGroup(armRenderers, armHighlightMat);
                HighlightGroup(robotRenderers, defaultMat);
                break;

              default:  // Reset the highlights
              
                ResetHighlights();
                break;
           }
        }

        else
        {
            ResetHighlights();
        }
    }


    private void HighlightGroup(Renderer[] renderers, Material mat)
    {
        foreach (Renderer renderer in renderers)
        {
            renderer.material = mat;
        }
    }
    

    private void ResetHighlights()
    {
        HighlightGroup(robotRenderers, defaultMat);
        HighlightGroup(armRenderers, defaultMat);
    }
}