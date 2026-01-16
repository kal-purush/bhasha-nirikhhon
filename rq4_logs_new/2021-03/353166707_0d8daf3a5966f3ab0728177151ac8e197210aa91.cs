using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DraggingScript : MonoBehaviour
{
    public string dragKey = "DragButton";
    public string mouseKey = "Fire1";
    public string draggableTag;
    public bool isDragging = false;

    public LayerMask draggableLayers;
    public LayerMask draggableReference;

    // Update is called once per frame
    void Update()
    {
        
            float keyValue = Input.GetAxis(dragKey);
            float mouseValue = Input.GetAxis(mouseKey);

            if (keyValue > 0 && !isDragging)
            {
                isDragging = true;

            }
            else if (keyValue <= 0 && isDragging)
            {
                isDragging = false;

            }
            else if (isDragging && mouseValue > 0)
            {
                CheckDrag();
            }
    }

    void CheckDrag()
    {
        Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;

        if (Physics.Raycast(ray, out hit, draggableLayers) && hit.transform.CompareTag(draggableTag))
        {
            RaycastHit planeHit;

            if (Physics.Raycast(ray, out planeHit, draggableReference))
            {

                hit.transform.position = new
                    Vector3(planeHit.point.x,
                    planeHit.point.y,
                    hit.transform.position.z);
            }
            //hit.transform.position = 
            //Destroy(hit.transform.gameObject);
        }
    }
}