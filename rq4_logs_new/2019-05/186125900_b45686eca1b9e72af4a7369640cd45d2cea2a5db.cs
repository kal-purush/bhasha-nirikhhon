using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyScript : MonoBehaviour
{

    public int target = 0; //goes to checkpoint zero, checkpoints will be in array
    public Transform exit; //stuff happens when enemy gets to this point
    public Transform[] waypoints;
    public float navUpdate;

    private Transform enemy;
    private float navTime = 0;
    // Start is called before the first frame update
    void Start()
    {
        enemy = GetComponent<Transform>();
    }

    // Update is called once per frame
    void Update()
    {
        if (waypoints!=null)
        {
            navTime += Time.deltaTime;
            if (navTime >navUpdate)
            {
                if (target < waypoints.Length)
                {
                    // build in methods moveTowards takes 3 args: current possition, target position, time
                    enemy.position = Vector2.MoveTowards(enemy.position, waypoints[target].position, navTime);
                }
                else
                {
                    enemy.position = Vector2.MoveTowards(enemy.position, exit.position, navTime);
                }
                navTime = 0;
            }
        }
    }
    //make this 2D, it wont otherwise you big dumb dumb
    void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.tag=="WayPoint")
        {
            target += 1;
        }
        else if (collision.tag== "Finnish")
        {
            GameManager.instance.EnemyReachedEnd();
            Destroy(gameObject);
        }
    }

}