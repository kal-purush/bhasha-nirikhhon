using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class MoveImageUsingMouse : MonoBehaviour, IPointerDownHandler, IPointerUpHandler,IDragHandler
{
/*    [SerializeField]
    private Canvas canvas;*/
    [SerializeField]
    private RectTransform rectTransform;


    public void OnPointerUp(PointerEventData eventData)
    {
    }

    public void OnPointerDown(PointerEventData eventData)
    {
        Debug.Log("inside mouse down");
    }

    public void OnDrag(PointerEventData eventData)
    {
        transform.position = eventData.position;
    }
}