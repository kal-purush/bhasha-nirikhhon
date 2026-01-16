using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerStartPosi : MonoBehaviour
{
    [SerializeField] GameObject player1;
    [SerializeField] GameObject player2;

    [SerializeField] GameObject spawnArea_player1;
    [SerializeField] GameObject spawnArea_player2;

    /// <summary>
    /// player 同士の最低距離 大きすぎるときは自動で小さくなります
    /// </summary>
    [SerializeField] float spaceLenge = 2;


    Vector3 point_player1;


    private void Start()
    {
        //// test
        //for (int i = 0; i < 100; i++)
        //{
        //    Spawn(player1, spawnArea_player1.transform.localScale, spawnArea_player1.transform.position);
        //}

        // test用
        float big = spawnArea_player1.transform.localScale.x;
        if (big < spawnArea_player1.transform.localScale.z)
        {
            big = spawnArea_player1.transform.localScale.z;
        }
        if (spaceLenge > big / 2)
        {
            spaceLenge = big / 2;
        }


        Player1_Move(player1, spawnArea_player1.transform.localScale, spawnArea_player1.transform.position);

        Player2_Move(player2, spawnArea_player2.transform.localScale, spawnArea_player2.transform.position);


        //Destroy(this);
    }

    public void TestMove()
    {

        Player1_Move(player1, spawnArea_player1.transform.localScale, spawnArea_player1.transform.position);

        Player2_Move(player2, spawnArea_player2.transform.localScale, spawnArea_player2.transform.position);
    }

    private void Player1_Move(GameObject orig, Vector3 area, Vector3 center)
    {
        point_player1 = center + new Vector3(Random.Range(-area.x / 2, area.x / 2), 0, Random.Range(-area.z / 2, area.z / 2));

        orig.transform.position = point_player1;
    }

    private void Player2_Move(GameObject orig, Vector3 area, Vector3 center)
    {
        Vector3 point_player2 = center + new Vector3(Random.Range(-area.x / 2, area.x / 2), 0, Random.Range(-area.z / 2, area.z / 2));
        while (spaceLenge > (point_player1 - point_player2).magnitude)
        {
            //
            Debug.Log((point_player1 - point_player2).magnitude);

            point_player2 = center + new Vector3(Random.Range(-area.x / 2, area.x / 2), 0, Random.Range(-area.z / 2, area.z / 2));
        }

        orig.transform.position = point_player2;
    }
}