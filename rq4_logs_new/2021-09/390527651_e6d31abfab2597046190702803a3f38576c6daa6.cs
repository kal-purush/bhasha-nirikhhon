using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GhostAnimator : MonoBehaviour
{
    private Animator anim;
    private Enemy enemy;
    Vector2 direction;

    private void Start()
    {
        anim = GetComponent<Animator>();
        enemy = GetComponent<Enemy>();
    }

    private void FixedUpdate()
    {
        Vector3 tempNext = enemy.directionDelta;

        if (tempNext.y > 0)
            anim.SetInteger("vertical", 1);
        else if (tempNext.y < 0)
            anim.SetInteger("vertical", -1);
        else
            anim.SetInteger("vertical", 0);

        if (tempNext.x > 0)
            anim.SetInteger("horizontal", 1);
        else if (tempNext.x < 0)
            anim.SetInteger("horizontal", -1);
        else
            anim.SetInteger("horizontal", 0);
    }
}