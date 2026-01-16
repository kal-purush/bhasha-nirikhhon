using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BreakableObject : MonoBehaviour
{
    [SerializeField]
    private SpriteRenderer sr;
    [SerializeField]
    private BoxCollider2D bc;

    public int index;

    void Start()
    {
        sr.sortingOrder = (int)transform.position.y * -2;

        sr.sprite = BreakablesDict.Instance.GetBreakable(index).full;
    }

    private void OnTriggerEnter2D(Collider2D collision) {
        bc.enabled = false;
        sr.sprite = BreakablesDict.Instance.GetBreakable(index).broken;
    }
}