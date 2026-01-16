using UnityEngine;

public class AttractedState : ITargetState
{
    private readonly StatePatternTarget target;

    public AttractedState(StatePatternTarget statePatternTarget)
    {
        target = statePatternTarget;
    }

    public void UpdateState()
    {

    }

    public void OnTriggerEnter(Collider other)
    {

    }

    public void Idle()
    {

    }

    public void FollowLeader()
    {

    }

    public void Repulsed()
    {

    }

    public void Patrol()
    {

    }

    public void Attracted()
    {

    }
}