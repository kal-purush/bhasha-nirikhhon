using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;

public class DisableObjOnBackgroundClick : MonoBehaviour, IPointerDownHandler
{
    [SerializeField] GameObject[] objsToDisable;
    public void OnPointerDown(PointerEventData pointerEventData)
    {
        Debug.Log("mouse click on bg");
        SetObjectsInactive();
    }

    private void OnMouseDown()
    {
        Debug.Log("mouse down!!");
    }

    public void SetObjectsInactive()
    {
        foreach (GameObject item in objsToDisable)
        {
            item.SetActive(false);
        }

        this.gameObject.SetActive(false);
    }
}