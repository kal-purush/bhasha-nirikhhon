using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MeshColliderKeeper : MonoBehaviour
{
    private MeshCollider collider;
    
    // Start is called before the first frame update
    void Start()
    {
        collider = GetComponent<MeshCollider>();
    }

    // Update is called once per frame
    void Update()
    {
        if (collider) return;
    }
}