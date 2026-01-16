using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NPCFlip : MonoBehaviour
{
    private GameObject player;
    private moveplayer direction;
    private SpriteRenderer sprite;
    public float distanceAway;
    // Start is called before the first frame update
    void Start()
    {
        player = GameObject.FindGameObjectWithTag("Player");
        direction = player.GetComponent<moveplayer>();
        sprite = GetComponent<SpriteRenderer>();
    }

    // Update is called once per frame
    void Update()
    {
        float distance = player.transform.position.x - transform.position.x;
        if ((Mathf.Abs(distance) < distanceAway) && direction.isFaceRight && Input.GetKeyDown(KeyCode.E))
        {
            sprite.flipX = true;
        }
        else if ((Mathf.Abs(distance) < distanceAway) && !direction.isFaceRight && Input.GetKeyDown(KeyCode.E))
        {
            sprite.flipX = false;
        }
    }
}