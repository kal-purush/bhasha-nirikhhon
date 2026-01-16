using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(LiquidControl))]
public class PourControl : MonoBehaviour
{
    [Range(0.0f, 180.0f)]
    public float pouringAngle = 90.0f;
    private LiquidControl _liquidControl;
    private GameObject _liquidParticlesGO;
    private ParticleSystem _liquidParticles;
    private ParticleSystem.EmissionModule _liquidEmission;
    private Renderer _particleRenderer;
    private void Start() {
        _liquidControl = GetComponent<LiquidControl>();
        
        _liquidParticlesGO = transform.Find("Liquid Emitter").gameObject;
        if (_liquidParticlesGO == null) {
            Debug.Log("You must have a Child called 'Liquid Emitter' with a particle system");
            return;
        }
        
        _liquidParticles = _liquidParticlesGO.GetComponent<ParticleSystem>();
        _liquidEmission = _liquidParticles.emission;

        _particleRenderer = _liquidParticles.GetComponent<Renderer>();
        if (_particleRenderer.material == null)
        {
            // Create a new material with a particle shader
            _particleRenderer.material = new Material(Shader.Find("Particles/Standard Unlit"));
        }
        else
        {
            // Ensure the existing material uses a particle shader
            _particleRenderer.material.shader = Shader.Find("Particles/Standard Unlit");
        }
    }

    void Update()
    {
        _particleRenderer.material.color = _liquidControl.colour;
        if (_liquidParticles != null)
        {
            Vector3 rotation = transform.forward;

            if (Vector3.Angle(Vector3.down, transform.forward) <= pouringAngle && !_liquidControl._isEmpty)
            {
                _liquidControl.Pour();
                _liquidEmission.enabled = true;
            }
            else
            {
                _liquidEmission.enabled = false;
            }
        }
    }
}