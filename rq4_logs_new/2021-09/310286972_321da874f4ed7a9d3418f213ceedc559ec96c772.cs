using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CarCount_M : MonoBehaviour
{
    private Mission1_M m1m;
    private ObjectStateManagement_Y osmY;
    private GameObject player;
    private bool kuruma;

    void Start()
    {
        player = GameObject.Find("Player");
        m1m = player.GetComponent<Mission1_M>();
        osmY = this.GetComponent<ObjectStateManagement_Y>();
    }
    void Update()
    {
        if (osmY.HP <= 0 && !kuruma)
        {
            m1m.CarDestroy();
            kuruma = true;
        }
    }
}