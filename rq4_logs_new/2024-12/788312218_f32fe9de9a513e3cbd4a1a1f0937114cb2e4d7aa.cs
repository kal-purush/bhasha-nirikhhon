using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CrabEnemyAnimations : MonoBehaviour
{
    // Start is called before the first frame update

    private Animator animator;

    private Rigidbody rigidbody;

    private GameObject grandParent;
    private GameObject parent;
    void Start()
    {
        animator = GetComponent<Animator>();

        parent = this.transform.parent.gameObject;
        grandParent = parent.transform.parent.gameObject;

        rigidbody.GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    void Update()
    {
        if (rigidbody.velocity.x != 0 | rigidbody.velocity.z != 0)
        {
            animator.SetFloat("speed", 1);
        }
        else
        {
            animator.SetFloat("speed", 0);
        }
    }

    public void PlayAttackAnimationCrab()
    {
        animator.SetTrigger("Attack");
    }
}