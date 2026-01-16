using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WheelRotation : MonoBehaviour
{
    private Vector3 direction;
    // Start is called before the first frame update
    void Start()
    {
        direction = Vector3.zero;
    }

    // Update is called once per frame
    void Update()
    {

        // move wheels when car moves
        direction.x = Input.GetAxis("Horizontal");
        direction.z = Input.GetAxis("Vertical");
        if (direction != Vector3.zero)
            transform.Rotate(new Vector3(5, 0, 0));
    }
}