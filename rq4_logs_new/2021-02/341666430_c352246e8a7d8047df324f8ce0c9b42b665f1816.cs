using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ParallaxEffect : MonoBehaviour
{
    public Transform[] backgroundLayers;
    public float smoothing = 1f;

    private float[] _parallaxScales;
    private Vector3 _previousCameraPosition;
    
    private void Start()
    {
        _previousCameraPosition = transform.position;

        _parallaxScales = new float[backgroundLayers.Length];
        for (int i = 0; i < backgroundLayers.Length; i++)
        {
            _parallaxScales[i] = backgroundLayers[i].position.z;
        }
    }

    private void Update()
    {
        setPositionOfEachBackground();
        _previousCameraPosition = transform.position;
    }

    private void setPositionOfEachBackground()
    {
        for (int i = 0; i < backgroundLayers.Length; i++)
        {
            Vector3 layerPosition = backgroundLayers[i].position;
            float parallax = (_previousCameraPosition.x - transform.position.x) * _parallaxScales[i];
            float backgroundTargetPositionX = layerPosition.x + parallax;
            Vector3 backgroundTargetPosition = new Vector3(backgroundTargetPositionX, layerPosition.y, layerPosition.z);
            backgroundLayers[i].position =
                Vector3.Lerp(layerPosition, backgroundTargetPosition, smoothing * Time.deltaTime);
        }
    }
}