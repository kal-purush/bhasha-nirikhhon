using UnityEngine;
using System.Collections;

public class KI_HealthController : MonoBehaviour
{
    public float health = 2;
    public float damage = 1;
    private HealthController healthController;
    // Use this for initialization
    void Start ()
    {
        healthController = GameObject.FindGameObjectWithTag(MyConst.player).GetComponent<HealthController>();
    }

    public void Damage(float damge)
    {
        health -= damage;

        if (health == 0)
            Destroy(gameObject);
    }
    
    void OnCollisionEnter2D(Collision2D col)
    {
        if (col.gameObject.tag == MyConst.player)
            healthController.Damage(damage);
    }

    void OnCollisionStay2D(Collision2D col)
    {
        if (col.gameObject.tag == MyConst.player)
            healthController.Damage(damage);
    }
}