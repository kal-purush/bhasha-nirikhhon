using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(Rigidbody2D))]
[RequireComponent(typeof(Collider2D))]
public class GrabbableItem : MonoBehaviour
{
    public float highlightRange;
    public float distanceToPlayer;
    public bool isClosest;

    private PlayerController player;

    private SpriteRenderer spriteRenderer;
    private Color originalColor;

    private Rigidbody2D rb;
    private Collider2D collider2D;

    private Transform holder;

    // Start is called before the first frame update
    void Start()
    {
        player = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerController>();
        spriteRenderer = GetComponent<SpriteRenderer>();
        originalColor = spriteRenderer.color;
        isClosest = false;

        rb = GetComponent<Rigidbody2D>();
        collider2D = GetComponent<Collider2D>();
    }

    // Update is called once per frame
    void Update()
    {
        // if holder is not null then this implicitly shows that this is the currently equipped (held) item
        if (holder != null)
        {
            transform.position = holder.position;
            return;
        }

        distanceToPlayer = Util.CalculateDistance(player.transform.position, transform.position);

        var color = originalColor;

        var currClosest = player.GetClosestGrabbableItem();
        var currClosestDistance = Mathf.Infinity;
        if (currClosest != null)
        {
            currClosestDistance = currClosest.distanceToPlayer;
        }

        if (distanceToPlayer <= highlightRange && currClosestDistance >= distanceToPlayer)
        {
            color = Color.green;
            isClosest = true;
            player.SetClosestGrabbableItem(this);
        } else if (isClosest)
        {
            player.SetClosestGrabbableItem(null);
        }

        spriteRenderer.color = color;
    }

    public void ResetItem()
    {
        holder = null;
        spriteRenderer.color = originalColor;
        isClosest = false;
    }

    public void Disable()
    {
        rb.simulated = false;
        collider2D.enabled = false;
    }

    public void Enable()
    {
        rb.simulated = true;
        collider2D.enabled = true;
    }

    public void Equip(Transform holder)
    {
        this.holder = holder;
    }
}