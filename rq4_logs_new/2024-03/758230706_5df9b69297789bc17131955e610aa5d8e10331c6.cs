using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using static UnityEngine.GraphicsBuffer;

public class EnemySpawn : MonoBehaviour
{
    public GameObject player1;
    public GameObject player2;
    public float speed;

    private float distance1;
    private float distance2;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        distance1= Vector2.Distance(transform.position,player1.transform.position);
        distance2 = Vector2.Distance(transform.position, player2.transform.position);
        Vector2 target;
        GameObject chaseTarget;

        //Vector2 direction = player1.transform.position - transform.position;
        if (distance1 > distance2)
        {
            chaseTarget = player2;
        }
        else  {
            chaseTarget = player1;
        }
        target = Vector2.MoveTowards(this.transform.position, chaseTarget.transform.position, speed * Time.deltaTime);
        transform.position = new Vector3(target.x, target.y, transform.position.z);
    }
}