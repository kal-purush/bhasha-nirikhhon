using UnityEngine;
using Pathfinding;

public class FleeingAStarMovement : AStarMovement
{

    void Start()
    {
        base.init();
    }

    public override bool preMoveChecksDone()
    {
        if (!base.preMoveChecksDone())
            return false;

        // Reverse the direction of the path to flee
        path.vectorPath.Reverse();

        return true;
    }
    private void FixedUpdate()
    {
        if (!preMoveChecksDone())
        {
            return;
        }

        // Move character towards next waypoint
        Vector3 direction = (path.vectorPath[currentWaypoint] - transform.position).normalized;
        enemy.moveInDirection(-direction);

        // Check if close enough to the current waypoint, then proceed to the next one
        float distance = Vector3.Distance(transform.position, path.vectorPath[currentWaypoint]);
        if (distance < nextWayPointDist)
        {
            currentWaypoint++;
        }
    }
}