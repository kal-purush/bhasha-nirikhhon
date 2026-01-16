using System.Collections;
using System.Collections.Generic;
using Unity.AI.Navigation;
using UnityEngine;
using UnityEngine.AI;

public class EnemyAI : MonoBehaviour
{
    public Transform player;
    private NavMeshAgent agent;
    public float stoppingDistance = 2f;
    private GravityInverter gravity;

    public NavMeshSurface surface;
    private AsyncOperation navMeshOperation;

    void Start()
    {
        agent = GetComponent<NavMeshAgent>();
        agent.stoppingDistance = stoppingDistance;
        agent.enabled = false;
        
        gravity = GetComponent<GravityInverter>();

        StartCoroutine(BuildNavMesh());
    }

    IEnumerator BuildNavMesh()
    {
        var data = new NavMeshData();
        var sources = new List<NavMeshBuildSource>();
        var bounds = new Bounds(surface.transform.position, Vector3.one * 500f);

        NavMeshBuilder.CollectSources(bounds, surface.layerMask, surface.useGeometry, surface.defaultArea, new List<NavMeshBuildMarkup>(), sources);
        navMeshOperation = NavMeshBuilder.UpdateNavMeshDataAsync(data, surface.GetBuildSettings(), sources, bounds);
        
        yield return navMeshOperation;

        if (navMeshOperation.isDone)
        {
            surface.navMeshData = data;
            agent.enabled = true;
        }
    }

    void Update()
    {
        // If the agent is enabled and NavMesh building is completed
        if (agent.enabled && navMeshOperation.isDone)
        {
            agent.SetDestination(player.position);
            if (Vector3.Distance(transform.position, player.position) <= stoppingDistance)
            {
                agent.isStopped = true;
            }
            else
            {
                agent.isStopped = false;
            }
        }

        // If gravity should rotate, deactivate the NavMeshAgent
        if (gravity.ShouldRotate && agent.enabled)
        {
            agent.enabled = false;
        }

        // If gravity should not rotate, reactivate the NavMeshAgent
        if (!gravity.ShouldRotate && !agent.enabled)
        {
            agent.enabled = true;
            StartCoroutine(BuildNavMesh());
        }
    }


}