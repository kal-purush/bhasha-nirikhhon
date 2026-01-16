using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BoxScript : MonoBehaviour
{

    public GameObject breakBoxPrefab;
    private SpriteRenderer spriteRenderer;
    private BoxCollider2D boxCollider2D;
    void Start()
    {
        spriteRenderer = GetComponent<SpriteRenderer>();
        boxCollider2D = GetComponent<BoxCollider2D>();
        
    }
    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.collider.tag == "Projectile")
        {
            spriteRenderer.enabled = false;
            boxCollider2D.enabled = false;
            Crumble();
            Invoke("ActivateBox", 10f);
        }
    }
    private void Crumble()
    {
        GameObject breakBox = Instantiate(breakBoxPrefab, transform.position, transform.rotation);
        Invoke("KillBox", 1f);
    }

    private void KillBox(GameObject box)
    {
        Destroy(box);
    }

    private void ActivateBox()
    {
        spriteRenderer.enabled = true;
        boxCollider2D.enabled = true;
    }
}