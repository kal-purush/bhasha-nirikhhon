using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(Animator))] 
public class PlayerAttack : MonoBehaviour
{

    //commented headers due to error: 
    //Attribute 'Header' is not valid on this declaration type. It is only valid on 'field' declarations.
    //[Header ("Keybinds")]
    [SerializeField] private KeyCode attackKey = KeyCode.LeftAlt;
    
    //[Header ("Animation")]
    [SerializeField] private Animator anim;
    
    //[Header("Attack Settings")]
    //attack speed
    //attack interval/reset/cooldown
    
    public enum AttackStates
    {
        idle,
        attack1, 
        attack2,
        attack3,
        attack4
    }

    public AttackStates attackState;


    private void Update()
    {
        PlayerInput();
    }

    private void FixedUpdate()
    {
    }

    private void PlayerInput() //handles input and state, may wish to separate state in the future
    {
        if (Input.GetKeyUp(attackKey))
        {
            if (attackState == AttackStates.idle)
            {
                anim.SetTrigger("attack1");
            } else if (attackState == AttackStates.attack1)
            {
                anim.SetTrigger("attack2");
            } else if (attackState == AttackStates.attack2)
            {
                anim.SetTrigger("attack3");
            } else if (attackState == AttackStates.attack3)
            {
                anim.SetTrigger("attack4");
            }
        }
    }
}