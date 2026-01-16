using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Trap : MonoBehaviour
{
    protected SpriteRenderer spriteRenderer;
    protected Animator animator;
    protected Collider2D coll;

    public float attackPower;

    public TrapDetectionZone detectionZone;

    public bool canDMG = false;


    // Start is called before the first frame update
    void Start()
    {
        spriteRenderer= GetComponent<SpriteRenderer>();
        animator= GetComponent<Animator>();
        coll = GetComponent<Collider2D>();
    }

    // Update is called once per frame
    void Update()
    {
        if (detectionZone.player != null)
        {
            float distance = Vector3.Distance(transform.position, detectionZone.player.transform.position);
            float opacity = 1 - Mathf.InverseLerp(0.5f, 1.9f, distance);
            opacity = Mathf.Clamp01(opacity); // Ensure opacity is between 0 and 1

            // Set the sprite's opacity
            Color color = spriteRenderer.color;
            color.a = opacity;
            spriteRenderer.color = color;
            if (distance < 0.2)
            {
                animator.SetBool("Active",true);
            }

        }
    }

    public void canDamange()
    {
        canDMG = true;
        Debug.Log("TRAP ON");
    }

    public void canNOTDamange()
    {
        canDMG = false;
        Debug.Log("TRAP Off");
        animator.SetBool("Active", false);
    }

    private void OnTriggerStay2D(UnityEngine.Collider2D collision)
    {
                if (collision.gameObject.tag == "PlayerShadow" && canDMG)
        {
            RiggedPlayerController playerController = collision.gameObject.GetComponentInParent<RiggedPlayerController>();
            playerController.takeDamage(attackPower, this.gameObject);
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "PlayerShadow" && canDMG)
        {
            RiggedPlayerController playerController = collision.gameObject.GetComponentInParent<RiggedPlayerController>();
            playerController.takeDamage(attackPower, this.gameObject);
        }
    }
}