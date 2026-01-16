using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DragToRotate : MonoBehaviour
{

    public bool isDragging = false;
    [SerializeField] float sensitivity = 360;

    float dragFrom;
    Vector3 rotateFrom;
    Vector3 origin;

    private void Start()
    {
        origin = new Vector3(Screen.width/2,Screen.height/2,0);
    }
    // Update is called once per frame
    void Update()
    {        
        if (Input.GetMouseButtonDown(0))
        {
            if (!isDragging)
            {
                StartDrag();
            }
        }
        else if (Input.GetMouseButtonUp(0))
        {
            StopDrag();
        }

        if (isDragging) {
            float dragTo = GetAngle(Input.mousePosition - origin);
            float angleDifference = dragTo - dragFrom;


            transform.RotateAroundLocal(Vector3.up, angleDifference * sensitivity);
            transform.LookAt(Vector3.zero);
       
        }
    }


    void StartDrag() {
        isDragging = true;
        dragFrom = GetAngle(Input.mousePosition-origin);
        rotateFrom = transform.eulerAngles;
    }

    void StopDrag() {
        isDragging = false;
    }

    float GetAngle(Vector3 point) {
        return Mathf.Atan2(point.y,point.x);
    }
}