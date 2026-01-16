using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Enemy : MonoBehaviour
{
    Rigidbody2D body;
    float speed = 1;
    float nextMoveTimer = 1;
    string currentAction = null;

	void Start ()
    {
        body = GetComponent<Rigidbody2D>();
        InvokeRepeating("ChooseAction", 0, nextMoveTimer);
	}
	
	void Update ()
    {
        switch (currentAction)
        {
            case "lockOn":
                MoveTowardsPlayer();
                break;
        }
	}

    private void ChooseAction()
    {
        if (Vector2.Distance(transform.position, GameHandler.Instance.player.transform.position) < 2)
        {
            currentAction = "lockOn";
        }
    }

    private void MoveTowardsPlayer()
    {
        body.velocity = new Vector2(speed, body.velocity.y);
    }

}