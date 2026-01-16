using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollow : MonoBehaviour
{
    public Transform target;
    public Vector3 offset;
    private moveplayer move;

    public float xOffset;
    [Range(1, 10)]
    public float smoothness;

    // Start is called before the first frame update
    void Start()
    {
        move = target.GetComponent<moveplayer>();
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        Vector3 targetPosition = target.position + offset;
        if (move.translation == 0)
        {
            targetPosition = target.position + offset;
        }
        else if (move.isFaceRight)
        {
            targetPosition.x += xOffset;
        }
        else
        {
            targetPosition.x -= xOffset;
        }
        Vector3 smoothPosition = Vector3.Lerp(transform.position, targetPosition, smoothness * Time.deltaTime);
        transform.position = smoothPosition;
    }
}