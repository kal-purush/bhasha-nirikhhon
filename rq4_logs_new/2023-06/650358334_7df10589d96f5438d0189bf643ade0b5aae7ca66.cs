using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(LineRenderer))]
public class LaserGun : MonoBehaviour
{
    public Transform LaserOrigin;
    public float gunRange = 50f;
    public float LaserDuration = 0.05f;

    private LineRenderer laserLine;

    // Start is called before the first frame update
    void Awake()
    {
        laserLine = GetComponent<LineRenderer>();
    }

    // Update is called once per frame
    void Update()
    {
        laserLine.SetPosition(0, LaserOrigin.position);
        RaycastHit hit;
        if (Physics.Raycast(LaserOrigin.position, LaserOrigin.forward, out hit, gunRange))
        {
            laserLine.SetPosition(1, hit.point);
        }
        else
        {
            laserLine.SetPosition(1, LaserOrigin.position + LaserOrigin.forward * gunRange);
        }
    }
}