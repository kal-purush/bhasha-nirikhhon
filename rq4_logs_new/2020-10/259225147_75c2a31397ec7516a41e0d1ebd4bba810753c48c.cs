using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class EnemyDefender : EnemyBrainBase
{
    Transform king;

    // Start is called before the first frame update
    new void Start()
    {
        base.Start();

        List<GameObject> enemies = new List<GameObject>(GameObject.FindGameObjectsWithTag("Enemy"));
        List<Transform> e_trans = new List<Transform>();
        enemies.ForEach(item => e_trans.Add(item.transform));
        king = e_trans[0];

        var k_dist = Vector3.Distance(transform.position, king.position);
        e_trans.ForEach(item =>
        {
            if (k_dist > Vector3.Distance(item.position, king.position))
            {
                king = item;
                k_dist = Vector3.Distance(transform.position, king.position);
            }
        });

        navMesh.stoppingDistance = EnemyProperty.ExtraAuraDistance;
        Default = ExMove;
        FindAction = ExMove;
    }

    void ExMove()
    {
        navMesh.SetDestination(king.position);
    }
}