using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyTester : MonoBehaviour
{
    public EnemyTemplate enemy;
    public Vector3 spawn;
    public GameObject player;

    // Start is called before the first frame update
    void Start()
    {
        enemy.Reset(spawn, player);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}