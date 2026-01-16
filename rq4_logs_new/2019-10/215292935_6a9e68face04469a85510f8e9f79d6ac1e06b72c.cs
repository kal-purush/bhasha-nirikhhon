using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class EnemyController : MonoBehaviour
{
    //NavMeshAgent steuern den Enemy über das NavMesh
    public NavMeshAgent agent;


    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        //Der Enemy soll immer zum Player laufen. BUG: LIEFERT AKTUELL NUR NULL
        //Vector3 test = GameObject.FindWithTag("Player").transform.position;

        //Läuft aktuell nur zum Punkt (0,0,0) da der Player nicht gefunden wird
        agent.SetDestination(Vector3.zero);
    }
}