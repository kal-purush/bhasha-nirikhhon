using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Assets.Scripts.Utility;

public class LaserBeam : MonoBehaviour
{
    private Transform target;
    public GameObject impactEffect;
    private bool frameFlag = false;
    private float strengthRatio = 1.0f;
    private Vector3 targetCenter;

    //has LineRenderer

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
        strengthRatio = ratio;
    }
    // Update is called once per frame
    void Update()
    {
        if (target == null)
        {
            Destroy(gameObject);
            return;
        }

        //this.DrawLaserBeam();
        if (!frameFlag)
        {
            DrawLaserBeam();
            frameFlag = true;
        }
        else
        {
            Hit();
        }
    }

    private void DrawLaserBeam()
    {
        Drawer.DrawEmpty(gameObject.GetComponent<LineRenderer>());
        Drawer.DrawLaserBeam(
            gameObject.GetComponent<LineRenderer>(),
            transform.position,
            targetCenter,
            0.2f,
            Color.red);
    }

    private void Hit()
    {
        DrawLaserBeam();
        var effect = Instantiate(impactEffect, target.position, target.rotation);
        Destroy(effect, 2f);

        target.gameObject.GetComponent<EnemyHealth>().DecreaseHealth(strengthRatio);

        Destroy(gameObject);
    }
}