using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    [SerializeField] private GameObject player;

    private SpriteRenderer player_sprite = null;
    private Animator player_animator = null;

    // Start is called before the first frame update
    void Start()
    {
        player_sprite = player.GetComponent<SpriteRenderer>();
        player_animator = player.GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        float horizontal_input = Input.GetAxis("Horizontal");
        float vertical_input = Input.GetAxis("Vertical");
        Vector3 movement = horizontal_input * new Vector3(0.1f, 0, 0) + vertical_input * new Vector3(0, 0.1f, 0);
        player.transform.Translate(movement);

        if(horizontal_input > 0)
        {
            player_sprite.flipX = true;
        }
        else if (horizontal_input < 0)
        {
            player_sprite.flipX = false;
        }

        if(horizontal_input != 0)
        {
            player_animator.Play("PlayerWalkingHorizontal");
        }
        else if (vertical_input > 0)
        {
            player_animator.Play("PlayerWalkingUp");
        }
        else if (vertical_input < 0)
        {
            player_animator.Play("PlayerWalkingDown");
        }
        else
        {
            player_animator.Play("PlayerIdle");
        }

    }
}