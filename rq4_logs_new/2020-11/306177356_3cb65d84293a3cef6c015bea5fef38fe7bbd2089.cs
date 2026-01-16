using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class drag : EventTrigger
{
    private bool dragging;
    private bool needToSnap;

    private Vector2 newPos = new Vector2();

    // Start is called before the first frame update
    void Start()
    {
        needToSnap = true;
    }

    // Update is called once per frame
    void Update()
    {
        if (dragging)
        {
            transform.position = new Vector2(Input.mousePosition.x, Input.mousePosition.y);
        }
    }

    public override void OnPointerDown(PointerEventData eventData)
    {
        dragging = true;
    }

    public override void OnPointerUp(PointerEventData eventData)
    {
        dragging = false;

        if (needToSnap)
        {
            transform.position = newPos;
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        Vector3 snapPos = collision.GetComponent<snapPosition>().getSnapPosition().position;
        newPos.x = snapPos.x;
        newPos.y = snapPos.y;
        needToSnap = true;
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        needToSnap = false;
    }

}