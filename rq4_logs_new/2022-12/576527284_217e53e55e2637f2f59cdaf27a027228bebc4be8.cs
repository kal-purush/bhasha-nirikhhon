using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class MonsterCtrl : MonoBehaviour
{
    [SerializeField] private NavMeshAgent agent;

    // 주인공 캐릭터의 Transform 컴포넌트를 저장할 변수
    [SerializeField] private Transform playerTr;

    void Start()
    {
        playerTr = GameObject.FindGameObjectWithTag("Player").GetComponent<Transform>();
        agent = GetComponent<NavMeshAgent>();
    }

    void Update()
    {
        agent.SetDestination(playerTr.position);
    }
}