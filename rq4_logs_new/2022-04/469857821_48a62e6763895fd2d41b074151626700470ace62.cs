using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RaycastShoot : MonoBehaviour
{
    //public int gunDamage = 1;
    //how often to fire
    //public float fireRate = .25f;
    //how far the array can be cast
    public float weaponRange = 50f;
    //apply force to hitted obj
    //public float hitForce = 100f;
    public Transform gunEnd; //mark the position where the laser begin
    //private Camera fpsCam;

    private WaitForSeconds shotDuration = new WaitForSeconds(.07f); //how long the laser remain in the menu after fire
    private AudioSource gunAudio; //play shotting effect
    private LineRenderer laserLine;//draw line in 3D between 2 points
    //private float nexFire; //the second fire 
    // Start is called before the first frame update
    void Start()
    {
        laserLine = GetComponent<LineRenderer>();
        laserLine.startWidth = 0.1f;
        laserLine.endWidth = 0.1f;
        //gunAudio = GetComponent<AudioSource>();
        //fpsCam = GetComponentInParent<Camera>(); //search for camera in the obj it attached to then each obj in the parent hierachy

    }

    // Update is called once per frame
    void Update()
    {
        //StartCoroutine(ShotEffect());

        //Vector3 rayOrigin = fpsCam.ViewportToWorldPoint(new Vector3(0.5f, 0.5f, 0));//0.5f to be at the exact center
        RaycastHit hit;

        //draw a line with line renderer
        laserLine.SetPosition(0, gunEnd.position); //gunEnd = empty obj attach to the end of the gun
        //if (Physics.Raycast(rayOrigin, fpsCam.transform.forward, out hit, weaponRange))
        if (Physics.Raycast(this.transform.position, this.transform.forward, out hit, weaponRange))
        {
            laserLine.SetPosition(1, hit.point);
        }
        else
        {
            //calculate the end position
            var endPosition = this.transform.position + (this.transform.forward * weaponRange);
            //set the end of the point instead of no-ending
            laserLine.SetPosition(1, endPosition);
            
        }
        laserLine.enabled = true;
    }
    

    private IEnumerator ShotEffect()
    {
        //play Audio here
        //gunAudio.Play();

        //enable laser line: will display after each shot. BUT CNIB can shot nonstop?
        laserLine.enabled = true;
        yield return shotDuration; //wait for .07 second
        laserLine.enabled = false;
    }
}