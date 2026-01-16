using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class Wander : MonoBehaviour
{

    /// <summary>
    /// Gets a random position to travel to inside the wander radius
    /// </summary>
    /// <param name="origin">The current location of the character</param>
    /// <param name="wanderRadius">The maximum distance the character can travel</param>
    /// <returns>The vector3 of a new location inside of the wander radius</returns>
    public Vector3 WanderLocation(Vector3 origin, float wanderRadius)
    {
        Vector3 newPosition = RandomNavSphere(origin, wanderRadius, -1);

        //fix to stop floating enemies
        newPosition.y = 0f;
        return newPosition;
    }

    /// <summary>
    /// Finds a random location inside a sphere
    /// </summary>
    /// <param name="origin">This is the current location</param>
    /// <param name="dist"> Size of the distance it may wander</param>
    /// <param name="layermask">Needs to be -1 in most cases</param>
    /// <returns>Returns the location of a random spot with the distance specified.</returns>
    private Vector3 RandomNavSphere(Vector3 origin, float dist, int layermask)
    {
        Vector3 randDirection = Random.insideUnitSphere * dist;

        randDirection += origin;

        NavMeshHit navHit;

        NavMesh.SamplePosition(randDirection, out navHit, dist, layermask);

        return navHit.position;
    }
}