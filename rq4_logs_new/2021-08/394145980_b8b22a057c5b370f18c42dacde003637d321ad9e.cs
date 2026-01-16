using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bullet : MonoBehaviour
{
    float speed = 7f;
    Transform target;
    Vector3 moveDirection;
    public float survival_time = 5f;
    float time_left;
    void Start()
    {
        target = GameObject.Find("Player").GetComponent<Transform>();
        moveDirection = (target.transform.position - transform.position).normalized * speed;
        time_left = survival_time;
    }

    // Update is called once per frame
    void Update()
    {
        time_left -= Time.deltaTime;
        transform.Translate(moveDirection * Time.deltaTime);

        if (time_left < 0)
        {
            Destroy(this.gameObject);
        }
    }
}