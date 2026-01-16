using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Pathfinding;

[RequireComponent(typeof(Enemy))]
public class AStarMovement : MonoBehaviour
{
    [Header("Debug")]

    public Enemy enemy;

    public Path path;
    public int currentWaypoint;
    public bool reachedEndOfPath;

    public Seeker seeker;

    [Header("Set these")]

    public float nextWayPointDist = 0.2f;


    public void init()
    {
        enemy = GetComponent<Enemy>();

        seeker = GetComponent<Seeker>();

        InvokeRepeating(nameof(UpdatePath), 0f, 0.5f);
    }


    public virtual void UpdatePath()
    {
        if (enemy.target == null)
        {
            return;
        }
        if (seeker.IsDone())
            seeker.StartPath(transform.position, enemy.target.position, OnPathComplete);
    }

    public virtual void OnPathComplete(Path p)
    {
        if (!p.error)
        {
            path = p;
            currentWaypoint = 0;
        }
    }

    public virtual bool preMoveChecksDone()
    {
        if (path == null || (enemy.state != Enemy.State.Moving && enemy.state != Enemy.State.Idle))
        {
            return false;
        }

        if (currentWaypoint >= path.vectorPath.Count)
        {
            reachedEndOfPath = true;
            return false;
        }
        else
        {
            reachedEndOfPath = false;
        }

        return true;
    }


}