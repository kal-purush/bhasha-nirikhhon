using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TargetingController : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        GameObject[] playerUnits = new GameObject[1000];
        GameObject[] enemyUnits = new GameObject[1000];

        foreach(GameObject pUnit in playerUnits)
        {
            foreach(GameObject eUnit in enemyUnits)
            {
                float distance = Vector3.Distance(pUnit.transform.position, eUnit.transform.position);

                if(distance <= 10000f)
                {
                    pUnit.SendMessage("setTarget", eUnit);
                    eUnit.SendMessage("setTarget", pUnit);
                }
            }
        }

    }

    // Update is called once per frame
    void Update()
    {
        
    }
}