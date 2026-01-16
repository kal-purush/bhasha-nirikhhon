using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LiquidControl : MonoBehaviour
{
    public Liquid liquid;
    public GameObject liquidGO;
    public Color newColour;
    
    [Range(0.45f, 0.55f)]
    public float _fillAmount;

    private void Start() {
        Renderer renderer = liquidGO.GetComponent<renderer>();
        renderer.material = new Material(renderer.material);
        renderer.SetColor("_TopColor", newColour);
    }
    private void Update() {
        liquid.fillAmount = _fillAmount;
    }
}