using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class moob : MonoBehaviour
{

    NavMeshAgent Player_Nav;
    GameObject Destination;

    // Start is called before the first frame update
    void Start()
    {
        //プレイヤーのNavMeshAgentを取得
        Player_Nav = GetComponent<NavMeshAgent>();
        //目的地のオブジェクトを取得
        Destination = GameObject.Find("Destination");
    }

    // Update is called once per frame
    void Update()
    {
        //目的地を設定
        Player_Nav.SetDestination(Destination.transform.position);
    }
}