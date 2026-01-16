using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Spawn : Instruction
{
    public GameObject alienSmallPrefab;
    public Vector3 location;
    private float timer = 3.0f;

    #region Methods

    public Spawn(Vector3 location, float timer, Entity entity) : base(entity)
    {
        this.location = location;
        this.timer = timer;
        // TODO: search for a more ideal way of loading the prefab
        alienSmallPrefab = Resources.Load("Prefabs/Entities/AlienSmall", typeof(GameObject)) as GameObject;
    }

    private void SpawnAlienSmall()
    {
        Object.Instantiate(alienSmallPrefab, location, Quaternion.identity);
    }

    override public void Execute()
    {
        timer -= Time.deltaTime;

        if (timer < 0)
        {
            // TODO: limit number of spawns per nest
            SpawnAlienSmall();
            timer = 3;
        }
    }

    #endregion
}