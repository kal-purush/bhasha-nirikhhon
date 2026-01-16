using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemySpawnBehavior : MonoBehaviour
{
    [SerializeField] private GameObject patrolHunter;
    [SerializeField] private GameObject followHunter;
    [SerializeField] private List<GameObject> spawnLocList;

    [SerializeField]
    int nPatrolHunters, nFollowHunters;
    // Start is called before the first frame update
    private void Awake()
    {
        spawnHunters();
    }
    void Start()
    {
        
        //EventBroadcaster.Instance.AddObserver(EventNames.ON_START_GAME, this.spawnHunters);
        //nPatrolHunters = 10;
        //nFollowHunters = 3;
    }

    // Update is called once per frame
    void Update()
    {

    }

    void spawnHunters()
    {
        spawnPatrolHunters();
        spawnFollowHunters();
    }

    void spawnPatrolHunters()
    {
        for(int i = 0; i < nPatrolHunters; i++)
        {
            int spawnerUseIndex = Random.Range(0, spawnLocList.Count);
            GameObject pHunterObj = GameObject.Instantiate(patrolHunter, spawnLocList[spawnerUseIndex].transform.GetChild(0).transform.position, Quaternion.identity, null);
            Transform target1 = spawnLocList[spawnerUseIndex].transform.GetChild(1).transform;
            Transform target2 = spawnLocList[spawnerUseIndex].transform.GetChild(2).transform;
            pHunterObj.GetComponent<UnityStandardAssets.Characters.ThirdPerson.AICharacterControl>().SetTarget(target1, target2);
            spawnLocList.RemoveAt(spawnerUseIndex);
        }
    }
    void spawnFollowHunters()
    {

    }
}