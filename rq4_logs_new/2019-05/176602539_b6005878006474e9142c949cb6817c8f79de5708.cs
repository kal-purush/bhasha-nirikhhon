using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Projectile : MonoBehaviour
{
    // Start is called before the first frame update
    protected Transform target;
    public GameObject impactEffect;
    private float strengthRatio = 1.0f;
    protected Vector3 targetCenter;

    public void SetTarget(Transform target)
    {
        this.target = target;
        var rend = target.GetComponent<Renderer>();
        if (rend == null)
        {
            rend = target.GetComponentInChildren<Renderer>();
        }
        this.targetCenter = rend.bounds.center;
    }
    public void SetStrengthRatio(float ratio)
    {
        this.strengthRatio = ratio;
    }
    // Update is called once per frame
    /*void Update()
    {
        if(target == null)
        {
            Destroy(gameObject);
            return;
        }

        var rend = target.GetComponent<Renderer>();
        if (rend == null)
        {
            rend = target.GetComponentInChildren<Renderer>();
        }

        var direction = rend.bounds.center - transform.position;
        var distanceThisFrame = speed * Time.deltaTime;

        if(direction.magnitude <= distanceThisFrame)
        {
            Hit();
            return;
        }

        transform.Translate(direction.normalized * distanceThisFrame, Space.World);
    }*/

    protected void Hit()
    {
        var effect = Instantiate(impactEffect, transform.position, transform.rotation);
        Destroy(effect, 2f);

        target.gameObject.GetComponent<EnemyHealth>().DecreaseHealth(strengthRatio);

        Destroy(gameObject);
    }
}