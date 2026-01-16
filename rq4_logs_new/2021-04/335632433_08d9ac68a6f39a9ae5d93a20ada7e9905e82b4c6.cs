using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class enemyLockOn : MonoBehaviour
{
    public GameObject enemyLock;
    private void Start()
    {
        enemyLock = GameObject.Find("Main Camera");
    }
    private GameObject Etarget;

    protected void OnTriggerEnter(Collider c)
    {
        if (c.gameObject.tag == "Enemy")
        {
            Etarget = c.gameObject;
        }
    }
    float distanceE;
    private void OnTriggerStay(Collider other)
    {
        Vector3 enemyPos = Etarget.transform.position;
        Ray ray = new Ray(enemyPos, transform.position);
        RaycastHit hit;


        if (Physics.Raycast(ray, out hit, distanceE))
        {
            float dist = Vector3.Distance(enemyPos, hit.point);
            if (distanceE > dist) distanceE = dist;
            Debug.Log(distanceE);
        }
        else

            Debug.DrawLine(enemyPos, transform.position, Color.magenta, 0f, false);
    }
    protected void OnTriggerExit(Collider c)
    {
        if (c.gameObject.tag == "Enemy")
        {
            Etarget = null;
        }
    }
    public GameObject getTarget()
    {
        return this.Etarget;
    }

    bool LockE = false;

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.Q))
        {
            if (LockE == false) LockE = true;
            else if (LockE == true) LockE = false;

            if (LockE == false)
            {
                //Debug.Log("LockE=false");
            }
            else if (LockE == true)
            {
                //Debug.Log("LockE=true");
                if (!Etarget)
                {
                }
                else
                {
                    enemyLock.GetComponent<FollowingCamera>().enemyFlag();
                    //this.transform.LookAt(Etarget.transform);
                }
            }
        }
        //if (LockE == false)
        //{
        //    Debug.Log("LockE=false");
        //}
        //else if (LockE == true)
        //{
        //    Debug.Log("LockE=true");
        //    if (!Etarget)
        //    {
        //    }
        //    else
        //    {
        //        enemyLock.GetComponent<FollowingCamera>().enemyFlag();
        //        //this.transform.LookAt(Etarget.transform);
        //    }
        //}
    }
}