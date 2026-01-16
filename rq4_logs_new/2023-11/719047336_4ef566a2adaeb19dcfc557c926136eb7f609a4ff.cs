using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class wall : MonoBehaviour
{

    public Transform wallObeject;
    public float offset;

    void Start()
    {
        float pos = Random.Range(-offset, offset);
        wallObeject.localPosition = new Vector3(pos, 0, 0);
    }
}