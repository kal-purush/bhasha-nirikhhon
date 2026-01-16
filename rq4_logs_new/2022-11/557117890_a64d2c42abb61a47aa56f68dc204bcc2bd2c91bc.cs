using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LightFlicker : MonoBehaviour
{
    Light lightComponent;
    float lastIntensity;
    float newIntensity;

    //The average of the two values with be aproximately where the intensity stays.
    //The greater in difference the two values are, the more intense the intensity change will be
    [Range(0, 2)]
    [SerializeField]
    float intensityChangeMin = 0f;
    
    [Range(0, 2)]
    [SerializeField]
    float intensityChangeMax = 2f;

    // Start is called before the first frame update
    void Start()
    {
        lightComponent = GetComponent<Light>();
        

    }

    // Update is called once per frame
    void Update()
    {
        lastIntensity = lightComponent.intensity;
        newIntensity = Random.Range(intensityChangeMin, intensityChangeMax);
        lightComponent.intensity = Mathf.Lerp(lastIntensity, newIntensity, Time.deltaTime);
    }
}