using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ResetPositionifInteractHeld : MonoBehaviour
{
    // Start is called before the first frame update
    Vector3 originalPosition;
    public Bounds bounds;
    private int counter = 0;
    public int counterThreshold = 60 * 3;

    void Start()
    {
        originalPosition = transform.position;    
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKey(KeyCode.E) || Input.GetButton("Xbox_X_Button"))
        {
            counter += 1;
        }
        else
        {
            counter = 0;
        }

        if (counter >= counterThreshold)
        {
            counter = 0;
            transform.position = originalPosition;
        }
    }
}