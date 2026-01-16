using UnityEngine;

public class CycleWaypoints : MonoBehaviour
{
    public SpawnTrigger spawnTrigger;
    public GameObject[] allWaypoints;

    private NavMeshAgent movingTarget;
    private int i = 0;


    void Start()
    {
        spawnTrigger = FindObjectOfType(typeof(SpawnTrigger)) as SpawnTrigger;
        allWaypoints = GameObject.FindGameObjectsWithTag("Waypoint");
        movingTarget = GetComponent<NavMeshAgent>();
        GoToNextWaypoint();
    }


    void GoToNextWaypoint()
    {
        if (i >= allWaypoints.Length)
        {
            //movingTarget.transform.LookAt(spawnTrigger.despawnPoint.transform);
            movingTarget.destination = spawnTrigger.despawnPoint.transform.position;
        }
        else
        {
            //movingTarget.transform.LookAt(allWaypoints[i].transform);
            movingTarget.destination = allWaypoints[i].transform.position;

            if (Vector3.Distance(movingTarget.transform.position, allWaypoints[i].transform.position) < 1f )
            {
                i++;
            }
        }
    }


    void Update()
    {
        GoToNextWaypoint();
    }

}