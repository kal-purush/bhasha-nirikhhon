using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpawnSmallAlien : Instruction
{
    public Vector3 location;
    public GameObject alienSmall;
    public GameObject alienQueen;
    public int nbAlienNeed;
    public float spawnTimer = 3.0f;
    private float timer = 1;

    #region Methods

    public SpawnSmallAlien(Vector3 location, float timer, Entity entity) : base(entity)
    {
        this.location = location;
        this.timer = timer;
    }

    private IEnumerator WaitSpawn()
    {
        yield return new WaitForSeconds(spawnTimer);
        SpawnSmall();
    }

    private void SpawnSmall()
    {
        //Instantiate the small aliens at the same position we are at
        //Object.Instantiate(alienSmall, location, Quaternion.identity);
        //Carry over some variable to Alien script?
        //alienSClone.GetComponent<AlienNest>().someVariable = GetComponent<AlienNest>().someVariable;
    }

    override public void Execute()
    {
        //StartCoroutine(WaitSpawn());
        //Object.Instantiate(instructionRunner, Vector3.back, Quaternion.identity);
    }
    #endregion
}