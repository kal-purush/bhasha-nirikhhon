using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SystemDopasowania : MonoBehaviour
{
    [SerializeField] private Transform correctForm;
    private bool isMoving;
    private bool isOnPlace;
    private Vector2 startPosition;
    private Vector2 resetPosition;
    [SerializeField] private ZwyciestwoDopasowania zwyciestwoDopasowania;

    private void Start()
    {
        resetPosition = this.transform.position;
        isOnPlace = false;
    }

    private void Update()
    {
        if (isMoving && !isOnPlace)
        {
            Vector3 mousePosition = Camera.main.ScreenToWorldPoint(Input.mousePosition);

            this.transform.position = new Vector3(mousePosition.x - startPosition.x, mousePosition.y - startPosition.y, 0);
        }
    }

    private void OnMouseDown()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Vector3 mousePosition = Camera.main.ScreenToWorldPoint(Input.mousePosition);
            startPosition = new Vector2(mousePosition.x - this.transform.position.x, mousePosition.y - this.transform.position.y);
            isMoving = true;
        }
    }

    private void OnMouseUp()
    {
        isMoving = false;

        if (Mathf.Abs(this.transform.position.x - correctForm.position.x) <= 0.5f && Mathf.Abs(this.transform.position.y - correctForm.position.y) <= 0.5f)
        {
            this.transform.position = new Vector3(correctForm.position.x, correctForm.position.y, 0);
            isOnPlace = true;
            zwyciestwoDopasowania.AddPoints();
        }
        else
        {
            this.transform.position = new Vector3(resetPosition.x, resetPosition.y, 0);
        }
    }
}