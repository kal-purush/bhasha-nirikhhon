using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HighlightController : MonoBehaviour
{
     public Material highlightMat;
    public Material defaultMat;
    public Renderer[] highlightRenderers;
    public LayerMask highlightLayer;


    
    // Start is called before the first frame update
    void Start()
    {
        if (highlightRenderers.Length > 0)
        {
            defaultMat = highlightRenderers[0].material;
        }
    }

    // Update is called once per frame
    void Update()
    {
        Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;

        if (Physics.Raycast (ray, out hit, Mathf.Infinity, highlightLayer))
        {
            if (hit.collider.gameObject == gameObject)
            {
                RobotHighlight(true);
            }
            else
            {
                RobotHighlight(false);
            }
        }
        else
        {
            RobotHighlight(false);
        }
    }

    private void RobotHighlight(bool highlight)
    {
        Material mat = highlight ? highlightMat : defaultMat;

        foreach (var renderer in highlightRenderers)
        {
            renderer.material = mat;
        }
    }
}