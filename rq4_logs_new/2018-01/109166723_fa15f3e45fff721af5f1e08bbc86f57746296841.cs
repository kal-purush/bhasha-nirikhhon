using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// To allow each particle to have a monobehaviour script for updating properties of said particle
public class ParticleUpdate : MonoBehaviour
{

    public Blane.Particle currentParticle;
    public Vector3 Size;


    void Start()
    {
        //currentParticle.position = transform.position;
        Size = transform.localScale;
    }

    void Update ()
    {
        //transform.position = currentParticle.position;
        //transform.position = currentParticle.Update(Time.deltaTime);
        transform.localScale = Size;
	}
}