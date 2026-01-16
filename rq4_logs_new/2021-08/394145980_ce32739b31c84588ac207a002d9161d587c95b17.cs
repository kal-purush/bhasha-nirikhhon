using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FollowWithDelay : MonoBehaviour
{
    //The gameobject this should follow behind
    [SerializeField] GameObject followedObject = null;
    private Transform followedTransform;
    //for simplicity, this uses physics frames (default 50/s)
    [SerializeField] int followDelay = 7;

    private readonly List<Vector3> positions = new List<Vector3>();

    // Start is called before the first frame update
    void Start()
    {
        followedTransform = followedObject.transform;
        for(int i = 0; i < followDelay; ++i)
        {
            positions.Insert(i, followedTransform.position);
        }
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        positions.Insert(followDelay - 1, followedTransform.position);
        transform.Translate(positions[1] - positions[0]);
        positions.RemoveAt(0);
    }
}