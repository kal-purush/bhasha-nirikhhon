using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TimeManager : MonoBehaviour
{
    public float timeRes;
    public float timeToDoSomething;

    // Start is called before the first frame update
    void Start()
    {
        timeRes = 10;
    }

    // Update is called once per frame
    void Update()
    {

        if (Time.time > timeToDoSomething)
        {
            Debug.Log("Hacer algo");
        }

        if (Input.GetKeyDown(KeyCode.Return))
        {
            timeRes = 0;
        }


    }
}