using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Blinky2 : FSMAgent
{
    void Start()
    {
        Initialize();//remove, this is testing
    }

    public override void Initialize()
    {
        currState = new ChaseState();
        currState.EnterState(this);
        currState.Start(this);
    }

    public override int GetCurrentAction()
    {
        // Check which direction currently moving in and assign action accordingly.
        Vector3 currPos = transform.position;
        Vector3 nextPos = ((ChaseState)currState).currTarget;
        float xDist = currPos.x - nextPos.x;
        float yDist = currPos.y - nextPos.y;
        Action currAction = Action.Up;
        if (Mathf.Abs(xDist) > Mathf.Abs(yDist))
        {
            if (xDist > 0)
            {
                currAction = Action.Left;
            }
            else
            {
                currAction = Action.Right;
            }
        }
        else
        {
            if (yDist > 0)
            {
                currAction = Action.Down;
            }
            else
            {
                currAction = Action.Up;
            }
        }

        return (int)currAction;
    }
}