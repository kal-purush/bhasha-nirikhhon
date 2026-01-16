using System.Collections;
using System.Collections.Generic;
using System.Runtime;
using UnityEngine;

public class Enemy1 : MonoBehaviour
{
    public int health;
    public float speed;

    private Animator anim;
    // Start is called before the first frame update
    void Start()
    {
       
    }

    // Update is called once per frame
    void Update()
    {
        transform.Translate(Vector2.left * speed * Time.deltaTime);
    }

    public void TakeDamage(int damage) {
        health -= damage;
        Debug.Log("damage TAKEN");
    }
}