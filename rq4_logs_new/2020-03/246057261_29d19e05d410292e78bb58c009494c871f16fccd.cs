using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LasBolt: MonoBehaviour
{

    private float speed;
    private float range;
    private Vector3 velocity;
    private Rigidbody bulletBody;
    private Vector3 originalPosition;

    void Start()
    {
        bulletBody = GetComponent<Rigidbody>();
    }

    public void Init(Vector3 direction, float speed, float range)
    {
        this.speed = speed;
        this.velocity = this.speed * direction;
        this.originalPosition = transform.position;
        this.range = range;
    }

    void FixedUpdate()
    {
        if (Vector3.Distance(this.originalPosition, transform.position) < range)
        {
            bulletBody.MovePosition(bulletBody.position + this.velocity * Time.fixedDeltaTime);
        }
        else
        {
            Destroy(this.gameObject);
        }
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.tag == "Entity" || other.tag == "Player")
        {
            Health health = other.GetComponent<Health>();
            if (health != null)
            {
                health.ChangeHealthBy(5);
            }
        }
        Destroy(this.gameObject);
    }
}