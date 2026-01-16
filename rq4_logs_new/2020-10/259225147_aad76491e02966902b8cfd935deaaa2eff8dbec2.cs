using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PortalParticleControl : MonoBehaviour
{
    [SerializeField] float force = 10;

    [SerializeField] GameObject pivot;

    new ParticleSystem particleSystem;
    bool isParticleReset = true;

    public bool IsStart { get; set; } = false;

    private void Start()
    {
        particleSystem = GetComponent<ParticleSystem>();
    }

    // Update is called once per frame
    void LateUpdate()
    {
        if (!IsStart) return;

        Arts_Process.HomingParticle(particleSystem, pivot, force);
    }
}