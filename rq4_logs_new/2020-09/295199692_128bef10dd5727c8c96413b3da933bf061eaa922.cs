using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(GrabbableItem))]
public class InverseGravity : MonoBehaviour
{
    private GrabbableItem gravityObject;

    // Start is called before the first frame update
    void Start()
    {
        gravityObject = GetComponent<GrabbableItem>();
        gravityObject.SetGravityScale(gravityObject.GetGravityScale() * -1);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}