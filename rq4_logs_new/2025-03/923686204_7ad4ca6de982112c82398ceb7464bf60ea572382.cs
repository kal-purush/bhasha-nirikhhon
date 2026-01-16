using System;
using System.Collections;
using UnityEngine;

public class FieldOfView : MonoBehaviour
{
    protected float radius;
    [Range(0,360)] protected float angle;

    //Layers
    protected LayerMask targetMask;
    protected LayerMask obstructionMask;

    protected bool canSeeTarget;



    protected IEnumerator FOVRoutine()
    {
        WaitForSeconds wait = new WaitForSeconds(0.2f);

        //This will changed
        while (true) 
        { 
            yield return wait;
            FieldOfViewCheck();
        }

    }

    private void FieldOfViewCheck()
    {
        Collider[] rangeCheck = Physics.OverlapSphere(transform.position, radius, targetMask);

        if (rangeCheck.Length != 0)
        {
            Transform target = rangeCheck[0].transform;
            Vector3 directionToTarget = (target.position - transform.position).normalized;
            
            if (Vector3.Angle(transform.position, directionToTarget) < angle / 2)
            {
                float distanceToTarget = Vector3.Distance(transform.position, target.position);

                if (!Physics.Raycast(transform.position, directionToTarget, distanceToTarget,obstructionMask)) 
                {
                    canSeeTarget = true;
                }
                else
                {
                    canSeeTarget = false;
                }
            }
            else 
            {
                canSeeTarget = false;
            }
        }
        else if (canSeeTarget){
            canSeeTarget = false;
        }
    }
}