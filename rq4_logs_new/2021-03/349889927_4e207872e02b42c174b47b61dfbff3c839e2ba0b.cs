using System;
using UnityEngine;

[Flags]
public enum CollisionTarget
{
    None = 0b_0000_0000,  // 0
    Dead = 0b_0000_0001,  // 1
    Goal = 0b_0000_0010,  // 2
}

public class BallCollisionChecker : MonoBehaviour
{
    private CollisionTarget _targets;

    void Start()
    {
        _targets = CollisionTarget.None;
    }

    private void OnTriggerEnter(Collider other)
    {        
        switch(other.gameObject.tag)
        {
            case "DeadZone":
                _targets |= CollisionTarget.Dead;
                break;
            case "GoalZone":
                _targets |= CollisionTarget.Goal;
                break;
        }
    }

    public bool IsCollisionWith(CollisionTarget target)
    {
        return (_targets & target) == target;
    }
}