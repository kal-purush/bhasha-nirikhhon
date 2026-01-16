using UnityEngine;
using System.Collections;

public class AITrojenHose : AIBase {

    public GameObject objectToSpawn;
    public float spawnRate = 2.5f;
    public int maxSpawnCount = 20;
    private bool isSpawning = false;
    private int currentSpawnCount = 0;
    private RailManager aiNodePathing;
    public bool spawnOnDeath = false;
    public Transform spawnPoint;

    public override void Start()
    {
        aiNodePathing = GameObject.FindObjectOfType<RailManager>();
        base.Start();
    }

	// Update is called once per frame
	public override void Update () {
        if (!isSpawning && currentSpawnCount < maxSpawnCount && !spawnOnDeath)
            StartCoroutine(Spawn());

        base.Update();
	}

    IEnumerator Spawn()
    {
        isSpawning = true;
        GameObject child = (GameObject)Instantiate(objectToSpawn, spawnPoint.position, spawnPoint.rotation);
        child.GetComponent<AIBase>().CurrentIndex = currentIndex;
        aiNodePathing.AddEntity(child);
        currentSpawnCount++;
        yield return new WaitForSeconds(spawnRate);
        isSpawning = false;
    }

    public override void DieWithGold()
    {
        if (spawnOnDeath)
        {
            for (int i = 0; i < maxSpawnCount; i++)
            {
                GameObject child = (GameObject)Instantiate(objectToSpawn, spawnPoint.position, spawnPoint.rotation);
                child.GetComponent<AIBase>().CurrentIndex = currentIndex;
                aiNodePathing.AddEntity(child);
            }
        }
        base.DieWithGold();
    }
}