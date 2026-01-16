using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Attack : Instruction
{

    private Destructible target;
   
    private Vector3 lastPosition;

    #region Methods

    public Attack(Destructible target, Entity entity): base(entity)
    {
        this.target = target;
        lastPosition = target.transform.position;
    }

    public override void Execute()
    {
        RaycastHit hit;
        if (Physics.Raycast(instructionRunner.transform.position, target.transform.position - instructionRunner.transform.position, out hit, Mathf.Infinity))
        {
            Debug.DrawRay(instructionRunner.transform.position, (target.transform.position - instructionRunner.transform.position) * 4f);

            Debug.Log("Ray connected with " + hit.collider.gameObject.name);
            Destructible raycastTarget = hit.collider.gameObject.GetComponent<Destructible>();
            if (raycastTarget != null)
            {
                if (raycastTarget == target)
                {
                    if (!target.IsDead())
                    {
                        Debug.Log(instructionRunner.name + " is attacking target " + target.name);
                        lastPosition = target.transform.position;
                    }
                    else
                    {
                        Debug.Log(target.name + " is dead. returning to previous behavior.");
                        instructionRunner.instructionEvent.Invoke(null);
                    }
                  
                }
            }
            else
            {
                Debug.Log(target.name + " has been lost. Beginning Chase.");
                instructionRunner.instructionEvent.Invoke(new Chase(lastPosition, instructionRunner));
            }
        }
        else
        {
            Debug.Log("Raycast did not connect.");
        }
        
    }
    #endregion
}