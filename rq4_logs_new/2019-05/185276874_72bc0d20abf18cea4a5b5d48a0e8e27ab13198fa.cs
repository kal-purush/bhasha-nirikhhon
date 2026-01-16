using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Enemy1Kretnja : MonoBehaviour
{
    public Transform enemyPosition;
    Vector3 lijeviStop,desniStop;
    bool premaLijevo;

    private void Start()
    {
        premaLijevo = true;
        lijeviStop = enemyPosition.localPosition + new Vector3(0, 0, -10);
        desniStop = enemyPosition.localPosition + new Vector3(0, 0, 10);
    }
    private void Update()
    {
        En1Kretnja();
    }
    void En1Kretnja()
    {
        if(enemyPosition.localPosition.z > lijeviStop.z && premaLijevo == true)
        {
            enemyPosition.Translate(0, 0, -3 * Time.deltaTime);
            if(enemyPosition.localPosition.z <= lijeviStop.z)
            {
                premaLijevo = false;
            }
        }
        if (enemyPosition.localPosition.z < desniStop.z && premaLijevo == false)
        {
            enemyPosition.Translate(0, 0, 3 * Time.deltaTime);
            if (enemyPosition.localPosition.z >= desniStop.z)
            {
                premaLijevo = true;
            }
        }
    }
}