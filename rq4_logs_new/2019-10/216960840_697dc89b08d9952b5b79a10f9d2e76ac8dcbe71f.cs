using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SP_Bullet : MonoBehaviour
{
    public float speed;
    public float timer;

    private void Start()
    {
        Destroy(gameObject, 10f);
    }

    void Update()
    {
        transform.position += transform.forward * speed * Time.deltaTime;
        if (timer > 0f)
        {
            timer -= Time.deltaTime;
        }
    }
}