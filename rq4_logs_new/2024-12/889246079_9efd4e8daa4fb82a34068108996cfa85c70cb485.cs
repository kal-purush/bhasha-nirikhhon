using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class tete : MonoBehaviour
{
    private Rigidbody2D rb;
    private Animator animator;

    public int velocidade;
    public float movimentoHorizontal;

    private void Awake()
    {
        rb = GetComponent<Rigidbody2D>();
        animator = GetComponent<Animator>();
    }

    private void Update()
    {
        Movimento();
        Animations();
        Flip();
    }

    private void Movimento()
    {
        movimentoHorizontal = Input.GetAxis("Horizontal");
        rb.velocity = new Vector2 (movimentoHorizontal * velocidade, rb.velocity.y);
    }

    private void Flip()
    {
        if (movimentoHorizontal > 0)
        {
            transform.localScale = new Vector3(1f,1f, 1f);
        }

        if (movimentoHorizontal < 0)
        {
            transform.localScale = new Vector3 (-1f,1f, 1f);
        }
    }

    private void Animations()
    {
        if (movimentoHorizontal == 0)
        {
            animator.Play("idle");
        }

        if (movimentoHorizontal != 0)
        {
            animator.Play("walk");
        }
    }
}