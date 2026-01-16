using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SetBallAndAiPositions : MonoBehaviour {

    private GameObject BallOfEvil;
    private BallBehavior BallOfEvilBehavior;

    private GameObject cpuPaddle;

    private void Start()
    {
        BallOfEvil = GameObject.Find("Ball of Evil");
        cpuPaddle = GameObject.Find("Paddle - CPU");
    }

    private void Update () {
		if (Input.GetKeyDown(KeyCode.U))
        {
            // Test CPU goal
            BallOfEvil.transform.position = GameObject.Find("CPU Goal").transform.position;
            BallOfEvil.GetComponent<Rigidbody>().velocity = new Vector3(-10.0f, 0.0f, 0.0f);
        }
        else if (Input.GetKeyDown(KeyCode.I))
        {
            // Test player goal
            BallOfEvil.transform.position = GameObject.Find("HMN Goal").transform.position;
            BallOfEvil.GetComponent<Rigidbody>().velocity = new Vector3(10.0f, 0.0f, 0.0f);
        }
        else if (Input.GetKeyDown(KeyCode.O))
        {
            BallOfEvil.GetComponent<Rigidbody>().velocity = new Vector3(5.0f, 0.0f, 10.0f);
        }
        else if (Input.GetKeyDown(KeyCode.P))
        {
            cpuPaddle.transform.position = new Vector3(-10.0f, 0.0f, 0.0f);
            cpuPaddle.GetComponent<Rigidbody>().velocity = Vector3.zero;
        }
        else if (Input.GetKeyDown(KeyCode.L))
        {
            cpuPaddle.transform.position = new Vector3(10000.0f, 100000.0f, 100000.0f);
            cpuPaddle.GetComponent<Rigidbody>().velocity = Vector3.zero;
        }
    }
}