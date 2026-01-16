using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TransFollower : MonoBehaviour {
    public Transform target;
    public bool trackX;
    public bool trackY;
    public bool trackZ;

    void Start () {
        if (!target)
            Debug.LogError("TransFollower has no target");
    }


    void Update () {
        if (!target)
            return;

        if (!trackX && !trackY && !trackZ)
            return;

        Transform myTrans = gameObject.GetComponent<Transform>();
        Vector3 targetPos = target.position;

        Vector3 pos = myTrans.position;
        if (trackX)
            pos.x = targetPos.x;
        if (trackY)
            pos.y = targetPos.y;
        if (trackZ)
            pos.z = targetPos.z;

        myTrans.position = pos;
    }
}