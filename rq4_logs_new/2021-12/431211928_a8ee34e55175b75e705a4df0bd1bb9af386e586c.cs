using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HoldObject : MonoBehaviour
{
    public bool canHold;
    public bool isHolding;
    public GameObject holdableObject;
    public float throwForce;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            if (!isHolding && canHold)
            {
                holdableObject.transform.parent = this.transform;
                holdableObject.GetComponent<Holdable>().holder = this.transform.gameObject;
                isHolding = true;
            }else if (isHolding)
            {
                holdableObject.transform.parent = null;
                holdableObject.GetComponent<Holdable>().holder = null;
                isHolding = false;
            }
        }

        if (Input.GetMouseButtonDown(1))
        {
            if (isHolding)
            {
                holdableObject.GetComponent<Rigidbody>().AddForce(this.transform.forward * throwForce, ForceMode.Impulse);
                holdableObject.transform.parent = null;
                holdableObject.GetComponent<Holdable>().holder = null;
                isHolding = false;
            }
        }
    }
}