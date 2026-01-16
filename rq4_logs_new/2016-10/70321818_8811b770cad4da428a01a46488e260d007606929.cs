using UnityEngine;
using System.Collections;

public class PlayerAnimation : MonoBehaviour
{

    Animator anim;
    [SerializeField]
    PlayerController player_controller;

    // Use this for initialization
    void Start()
    {
        anim = GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        var state = player_controller.player_state;
        anim.SetInteger("player_state", (int)state);
        var vec = player_controller.vec;
        anim.SetFloat("up_down_vec", vec.y);
        anim.SetFloat("right_left_vec", vec.x);

        Debug.Log(vec.y);

    }
}