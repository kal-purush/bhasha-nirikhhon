using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ThrowingBehaviour : MonoBehaviour
{

    public GameObject ball;

    public GameObject player;

    public Rigidbody ballRB;
    // Start is called before the first frame update
    void Start()
    {
        ball.SetActive(false);

        ballRB = ball.GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    void Update()
    {
        if(Input.GetKeyDown(KeyCode.Space))
        {
            ball.SetActive(true);
            ball.transform.rotation = player.transform.rotation;
            ballRB.AddForce(new Vector3(4, 4, 0), ForceMode.Impulse);

        }
    }
}