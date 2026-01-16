using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CheckInsideTheColider : MonoBehaviour
{

    void Update()
    {
        //Collider[] hitColidliders = Physics.OverlapSphere(transform.position, 0f);
        //foreach (Collider hitColider in hitColidliders)
        //{
        //    Debug.Log(hitColider.gameObject.name);
        //}
    }

    public bool CheckColIns(Collider c)
    {
        Collider[] hitColidliders = Physics.OverlapSphere(transform.position, 0f);
        for (int i = 0; i < hitColidliders.Length; i++)
        {
            if (hitColidliders[i] == c)
            {
                return true;
            }
        }
        return false;
    }

    public bool Check(Vector3 point)
    {
        Debug.Log((this.transform.position - point).magnitude);

        if ((this.transform.position - point).magnitude < 1)
        {
            return true;
        }
        return false;
    }

    public bool IsInside(Vector3 point)
    {
        
        Vector3 closest = this.GetComponent<Collider>().ClosestPoint(point);
        // Because closest=point if point is inside - not clear from docs I feel
        return closest == point;
    }
}