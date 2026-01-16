using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Enemy : MonoBehaviour
{
    EnemyController controller;
    // Start is called before the first frame update
    void Start()
    {
        controller = GetComponentInParent<EnemyController>();
    }

    // Update is called once per frame
    void Update()
    {
        #region Patrol
    
        transform.position = Vector2.MoveTowards(transform.position,
            controller.patrolLocations[controller.nextPatrolLocation].position,
            controller.stats.speed * Time.deltaTime);

        if (Vector2.Distance(transform.position, controller.patrolLocations[controller.nextPatrolLocation].position) <= 1)
            controller.nextPatrolLocation = (controller.nextPatrolLocation + 1) % controller.patrolLocations.Capacity;
        
        #endregion
    }

}