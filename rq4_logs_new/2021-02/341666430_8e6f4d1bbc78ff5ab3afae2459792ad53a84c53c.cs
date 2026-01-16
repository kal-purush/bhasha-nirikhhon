using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Random = UnityEngine.Random;

public class MinionMovement : MonoBehaviour
{
    public GameObject player;
    public float accelerationTime = 2f;

    private Vector2 _movement;
    private float _timeLeft;
    private Rigidbody2D _rb;

    private void Start()
    {
        _rb = GetComponent<Rigidbody2D>();
    }

    private void Update()
    {
        _timeLeft -= Time.deltaTime;
        if (_timeLeft <= 0)
        {
            _rb.velocity = new Vector2(Random.Range(-2f, 2f), 0);
            _timeLeft += accelerationTime;
        }

        if (transform.position.x < player.transform.position.x - 10)
        {
            Destroy(gameObject);
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player"))
        {
            _rb.velocity = new Vector2(-10, 0);
            GameMenager.Points++;
            GetComponent<ParticleSystem>().Play();
            //tutaj serduszka
            Debug.Log("Jabłko");
        }
    }
}