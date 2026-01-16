#nullable enable
using UnityEngine;
using UnityEngine.Rendering.Universal;

public class LightRandomizer : MonoBehaviour
{
    [Header("Dependencies")]
    
    [SerializeField] private Light2D? target;
    
    [Header("Parameters")]
    
    [SerializeField] private float min = 0.9f;
    [SerializeField] private float max = 1.1f;
    [SerializeField] private float frequency = 5;
    
    [Header("State")]
    
    [SerializeField] private float current;

    private void Update()
    {
        if (target == null) return;
        current += Time.deltaTime * frequency;
        target.intensity = Mathf.PerlinNoise1D(current) * (max - min) + min;
    }
}