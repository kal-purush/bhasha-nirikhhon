using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GhostTouch : MonoBehaviour
{
    [SerializeField] private GameObject reference;
    private Renderer _objectRenderer;
    
    public static int AmplitudeID = Shader.PropertyToID("_Amplitude");
    public static int FrequencyID = Shader.PropertyToID("_Frequency");
    public static int SpeedID = Shader.PropertyToID("_AnimationSpeed");

    private float _currentAmp, _currentFre, _currentSpe;
    private float _desiredAmp, _desiredFre, _desiredSpe;

    private void Awake()
    {
        _objectRenderer = reference.GetComponent<Renderer>();

        _currentAmp = 0; //max 1
        _currentFre = 1; //max 8
        _currentSpe = 0; //max 5

        _desiredAmp = 0;
        _desiredFre = 1;
        _desiredSpe = 0;
    }

    private void OnTriggerEnter(Collider other)
    {
        _desiredAmp = 1;
        _desiredFre = 8;
        _desiredSpe = 5;
    }

    private void OnTriggerExit(Collider other)
    {
        _desiredAmp = 0;
        _desiredFre = 1;
        _desiredSpe = 0;
    }

    private void Update()
    {
        _currentAmp = Mathf.Lerp(_currentAmp, _desiredAmp, Time.deltaTime * 3f);
        _currentFre = Mathf.Lerp(_currentAmp, _desiredFre, Time.deltaTime);
        _currentSpe = Mathf.Lerp(_currentSpe, _desiredSpe, Time.deltaTime * 4f);
        
        /*_objectRenderer.material.SetFloat(AmplitudeID, _currentAmp);
        _objectRenderer.material.SetFloat(FrequencyID, _currentFre);
        _objectRenderer.material.SetFloat(SpeedID, _currentSpe);*/
        
        _objectRenderer.material.SetFloat(AmplitudeID, _desiredAmp);
        _objectRenderer.material.SetFloat(FrequencyID, _desiredFre);
        _objectRenderer.material.SetFloat(SpeedID, _desiredSpe);
    }
}