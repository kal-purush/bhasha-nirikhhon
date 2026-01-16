using UnityEngine;

public class Lamp : MonoBehaviour
{
    public float intensity = 1.0f;
    public float amplitude = 1.0f;
    public float frequency = 1.0f;
    public float phase = 0.0f;
    public float fadeTime = 6.0f;

    private Light lightComponent;
    private float currentIntensity = 0.0f;
    private float fadeStartTime = 0.0f;

    private void Start()
    {
        lightComponent = GetComponent<Light>();
        currentIntensity = 0.0f;
        lightComponent.intensity = currentIntensity;
        fadeStartTime = Time.time;
    }

    private void Update()
    {
        float timeSinceFadeStart = Time.time - fadeStartTime;
        float fadeAmount = Mathf.Clamp01(timeSinceFadeStart / fadeTime);

        float sineWaveValue = Mathf.Sin((Time.time * frequency) + phase) * amplitude;
        currentIntensity = Mathf.Lerp(0.0f, intensity + sineWaveValue, fadeAmount);
        lightComponent.intensity = currentIntensity;
    }
}