using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemySpawner : MonoBehaviour
{

    public GameObject enemyPrefab;
    public int waveNumber = 0;
    public int waveMultiplier = 2;
    public int enemiesLeft = 0;

    private bool waveOngoing = false;
    private int enemyCap = 0;
    private List<GameObject> spawnList;

    // Start is called before the first frame update
    void Start()
    {
        Debug.Log("Press SPACEBAR to start enemy spawner wave test");
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown("space"))
        {
            if (waveOngoing == false)
            {
                waveOngoing = true;
                waveNumber += 1;
                enemyCap = waveNumber * waveMultiplier;

                int i = 0;
                for(i = 0; i < enemyCap; i ++)
                {
                    GameObject tester = Instantiate(enemyPrefab, new Vector3(Random.Range(-10.0f, 10.0f), 0, Random.Range(-10.0f, 10.0f)), Quaternion.identity);
                    spawnList.Add(tester);
                    enemiesLeft += 1;
                }
                Debug.Log("All enemies have spawned. They will all now die and next wave will start for demo purposes.");
                i = 0;
                for (i = 0; i < enemyCap; i++)
                {
                    Destroy(spawnList[i]);
                    enemiesLeft -= 1;
                }
            }
        }
    }
}