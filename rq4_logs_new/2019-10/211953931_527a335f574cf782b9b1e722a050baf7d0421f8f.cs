using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Drawer : MonoBehaviour
{
    public bool isOpen = false;
    public bool isLocked = false;
    private bool isMoving = false;
    private float distance = 6f;
    private float speed = .05f;
    private Vector3 InitialPos;

    void Start()
    {
        InitialPos = transform.localPosition;
        if (isOpen)
        {
            transform.localPosition = new Vector3 (InitialPos.x, InitialPos.y + distance, InitialPos.z);
        }
    }
    // Update is called once per frame
    void OnMouseDown()
    {
        if (!isLocked && !isMoving)
        {
            if (isOpen)
            {
                StartCoroutine("CloseDrawer");
            }
            else
            {
                
                StartCoroutine("OpenDrawer");
            }

        }
    }


    public IEnumerator OpenDrawer()
    {
        Debug.Log("opening!!!");
        isOpen = true;
        isMoving = true;

        while(transform.localPosition.y < (InitialPos.y + distance))
        {

            transform.Translate(0f, speed, 0f, Space.Self);
            Debug.Log(transform.localPosition.y);
            yield return null;
        }
        //code

        isMoving = false;
    }

    public IEnumerator CloseDrawer()
    {
        isMoving = true;
        isOpen = false;
        while (transform.localPosition.y > InitialPos.y)
        {
            transform.Translate(0f, -speed, 0f, Space.Self);
            yield return null;
        }
        transform.localPosition = InitialPos;
        isMoving = false;
    }

}