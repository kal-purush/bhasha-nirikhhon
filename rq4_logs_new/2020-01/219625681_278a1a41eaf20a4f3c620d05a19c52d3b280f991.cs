using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Swappable : MonoBehaviour
{
    public int swapID;

    public void Update()
    {
        // Check if the left mouse button was raised
        if (Input.GetMouseButtonUp(0))
        {
            // Check if the mouse was clicked over a UI element
            if (CheckBounds())
            {
                if (Draggable.holding)
                {
                    Draggable heldDrag = Draggable.heldObj.GetComponent<Draggable>();
                    Draggable thisDrag = GetComponent<Draggable>();
                    if (heldDrag.swapID == swapID)
                    {
                        Vector3 temp = heldDrag.startPos;
                        heldDrag.startPos = thisDrag.startPos;
                        thisDrag.startPos = temp;
                        heldDrag.dropPos = Draggable.heldObj.transform.position;
                        thisDrag.dropPos = transform.position;
                        thisDrag.isGoingBack = true;
                        heldDrag.isGoingBack = true;
                    }
                }
            }
        }
    }

    public bool CheckBounds()
    {
        Vector3 pos = transform.position;
        Vector3 mousePos = Input.mousePosition;
        Vector3 delta = mousePos - pos;
        float width = GetComponent<RectTransform>().rect.width;
        float height = GetComponent<RectTransform>().rect.height;
        if (delta.x < width / 2 && delta.x > -width / 2 &&
            delta.y < height / 2 && delta.y > -height / 2)
        {
            return true;
        }
        return false;
    }
}