using System;
using System.Collections;
using System.Collections.Generic;
using TreeEditor;
using UnityEditor.UIElements;
using UnityEngine;

public class Enemy : MonoBehaviour
{
    public float speed;
    public float maxSpeed;
    void Update()
    {
        transform.LookAt(Player.instance.transform);
        GetComponent<Rigidbody>().AddForce(transform.forward * (speed * Time.deltaTime));
        GetComponent<Rigidbody>().velocity = Vector3.ClampMagnitude(GetComponent<Rigidbody>().velocity, speed);
    }

    private void OnCollisionEnter(Collision collision)
    {
        var player = collision.gameObject.GetComponent<Player>();
        if (player != null)
        {
            player.Damage(1);
            Destroy(gameObject);
        }
    }
}