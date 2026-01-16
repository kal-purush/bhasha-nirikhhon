using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{

    public GameObject player;
    
    public int loomNumber;

    public Transform[] startingPositions = new Transform[3];

    void Awake()
    {
        if (loomNumber < 0)
        {
            loomNumber = 0;
        } else if (loomNumber > 2)
        {
            loomNumber = 2;
        }

        player.transform.position = startingPositions[loomNumber].position;
        player.transform.forward = startingPositions[loomNumber].forward;

    }
}