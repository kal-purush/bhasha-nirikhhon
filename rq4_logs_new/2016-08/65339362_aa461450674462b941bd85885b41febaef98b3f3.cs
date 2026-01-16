using UnityEngine;
using System.Collections;

public class ProjectileCollision : MonoBehaviour {

    [Header("Target")]
    public bool hitTarget;
    public GameObject target;

    [Header("Effects")]
    public GameObject effectPoint;
    [Space]
    public bool onStart;
    public GameObject startEffect;
    [Space]
    public bool onAlways;
    public GameObject alwaysEffect;
    [Space]
    public bool onDestroy;
    public GameObject destroyEffect;

    void Start()
    {
        if (onStart)
        {
            GameObject effects = GameObject.Instantiate(startEffect, effectPoint.transform.position, effectPoint.transform.rotation) as GameObject;
        }
        
    }

    void FixedUpdate()
    {
        if (onAlways)
        {
            GameObject effects = GameObject.Instantiate(alwaysEffect, effectPoint.transform.position, effectPoint.transform.rotation) as GameObject;
        }
    }

    void OnTriggerEnter(Collider col)
    {
        if (hitTarget && col.gameObject)
        {
           if (onDestroy)
           {
                GameObject effects = GameObject.Instantiate(destroyEffect, effectPoint.transform.position, effectPoint.transform.rotation) as GameObject;
           }

           Destroy(this.gameObject);
            
        }
    }
}