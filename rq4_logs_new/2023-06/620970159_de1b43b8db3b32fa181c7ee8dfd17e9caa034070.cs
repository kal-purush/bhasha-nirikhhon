using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ImpalerMovement : MonoBehaviour
{
    [SerializeField] EnemyController impaler;
    [SerializeField] ImpalerMovement impalerGraphic;
    public Animator animator;
    private Vector3 direction;
    // Start is called before the first frame update
    void Start()
    {
        animator = GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        SetAnimation();
    }

    void SetAnimation()
    {
        
            //animator.SetBool("isWalking", true);
            direction = impaler.enemyAgent.desiredVelocity;
            animator.SetFloat("x", direction.x);
            animator.SetFloat("y", direction.y);

    }
}