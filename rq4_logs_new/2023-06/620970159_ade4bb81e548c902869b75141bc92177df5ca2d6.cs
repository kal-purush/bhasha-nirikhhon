using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class KnockBack : MonoBehaviour
{
    [SerializeField] Rigidbody2D rb;
    [SerializeField] float strength = 1, delay = 0.5f;

    public void AddKnockBack(GameObject attacker)
    {
        StopAllCoroutines();
        Vector2 direction = (transform.position - attacker.transform.position).normalized;
        Debug.Log("heree");
        rb.AddForce(direction * strength, ForceMode2D.Impulse);
        StartCoroutine(Reset());
    }

    private IEnumerator Reset()
    {
        yield return new WaitForSeconds(delay);
        rb.velocity = Vector2.zero;
    }
}