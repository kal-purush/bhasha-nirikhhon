using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UIElements;

public class MoveTowardsPlayer : MonoBehaviour
{
    public BoxCollider2D playerHitbox;
    public GameObject npc;
    public float speed = 0.2f;
    void Start()
    {
        playerHitbox = GameObject.Find("Player").GetComponent<BoxCollider2D>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    void FixedUpdate()
    {
        MoveTowardsPlayerLogic();
    }

    private void MoveTowardsPlayerLogic()
    {
        float offsetY = playerHitbox.GetComponent<BoxCollider2D>().offset.y;
        transform.position = Vector2.MoveTowards(transform.position, new Vector2(playerHitbox.transform.position.x, playerHitbox.transform.position.y + offsetY), speed * Time.deltaTime);
    }
}