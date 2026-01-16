using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GrapplingHook : MonoBehaviour
{
    [SerializeField]
    public GameObject Hook;
    [SerializeField]
    public GameObject HookHolder;

    [SerializeField]
    float hookTravelSpeed;
    [SerializeField]
    float playerTravelSpeed;

    static bool fired;
    public bool hooked;
    
    //public GameObject HookedObject;

    float currentDistance;
    [SerializeField]
    float maxDistance;

    private bool grounded;

    float vertices;

    void Start()
    {
        fired = false;
    }

    void Update()
    {
        if (fired == false && Input.GetKey(KeyCode.S))
        {
            fired = true;

            if (fired)
            {
                LineRenderer rope = GetComponent<LineRenderer>();
                vertices = rope.positionCount;
                rope.SetPosition(0, HookHolder.transform.position);
                rope.SetPosition(1, Hook.transform.position);
            }

            if (fired == true && hooked == false)
            {
                Hook.transform.Translate(Vector3.forward * Time.deltaTime * hookTravelSpeed);
                currentDistance = Vector3.Distance(transform.position, Hook.transform.position);

                if (currentDistance >= maxDistance)
                {
                    returnHook();
                }
            }

            //if (hooked == true && fired == true)
            //{
            //    Hook.transform.parent = HookedObject.transform;

            //    transform.position = Vector3.MoveTowards(transform.position, Hook.transform.position, Time.deltaTime * playerTravelSpeed);
            //    float distanceToHook = Vector3.Distance(transform.position, Hook.transform.position);

            //    this.GetComponent<Rigidbody>().useGravity = false;

            //    if (distanceToHook < 1)
            //    {
            //        if(grounded == false)
            //        {
            //            if (this.transform.position.y > HookedObject.transform.position.y)
            //            {
            //                this.transform.Translate(Vector3.forward * Time.deltaTime * 13.0f);
            //                this.transform.Translate(Vector3.down * Time.deltaTime * 18.0f);
            //            }
            //            this.transform.Translate(Vector3.forward * Time.deltaTime * 13.0f);
            //            this.transform.Translate(Vector3.up * Time.deltaTime * 18.0f);
            //        }

            //        StartCoroutine("Climb");
            //    }
            //}
            //else
            //{
            //    Hook.transform.parent = HookHolder.transform;
            //    this.GetComponent<Rigidbody>().useGravity = true;
            //}
        }
    }

    IEnumerator Climb()
    {
        yield return new WaitForSeconds(0.1f);
        returnHook();
    }

    void returnHook()
    {
        Hook.transform.rotation = HookHolder.transform.rotation;
        Hook.transform.position = HookHolder.transform.position;
        fired = false;
        hooked = false;

        LineRenderer rope = GetComponent<LineRenderer>();
        vertices = rope.positionCount;
    }

    void checkIfGrounded()
    {
        RaycastHit hit;
        float dist = 1.0f;
        Vector3 dir = new Vector3(0, -1);

        if (Physics.Raycast(transform.position, dir, out hit, dist))
        {
            grounded = true;
        }
        else
        {
            grounded = false;
        }
    }
}