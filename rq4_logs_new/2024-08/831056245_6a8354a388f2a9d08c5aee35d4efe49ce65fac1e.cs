using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LiquidControl : MonoBehaviour
{
    public Liquid liquid;
    public Color newColour;
    
    [Range(0.44f, 0.57f)]
    public float _fillAmount;

    private Material _liquidMat;

    private void Start() {
        Renderer renderer = GetComponent<Renderer>();
        _liquidMat = new Material(renderer.material);

        renderer.material = _liquidMat;
        _liquidMat.SetColor("_BottomColor", newColour);
        _liquidMat.SetColor("_TopColor", newColour);
    }
    private void Update() {
        liquid.fillAmount = _fillAmount;
    }

    private void UpdateColour() {

    }


    public void Fill() {
        _fillAmount -= 0.005f;
        Debug.Log("Fill method called. New fill amount: " + _fillAmount);
    }

    private void Empty() {

    }

    // private void OnTriggerEnter(Collider other) {
    //     Debug.Log("Trigger entered by: " + other.gameObject.name); // Log the object that triggered it
    //     if (other.CompareTag("LiquidParticle")) {
    //         Debug.Log("LiquidParticle detected.");
    //         Fill();
    //     } else {
    //         Debug.Log("Other object detected with tag: " + other.tag);
    //     }
    // }

    void OnParticleCollision(GameObject other)
    {
        // Get the ParticleSystem component from the colliding object
        ParticleSystem particleSystem = other.GetComponent<ParticleSystem>();

        if (particleSystem != null)
        {
            Debug.Log("Trigger entered by: " + other.gameObject.name); // Log the object that triggered it
            if (other.CompareTag("LiquidParticle")) {
                Debug.Log("LiquidParticle detected.");
                Fill();
            } else {
                Debug.Log("Other object detected with tag: " + other.tag);
            }
        }
    }
}
