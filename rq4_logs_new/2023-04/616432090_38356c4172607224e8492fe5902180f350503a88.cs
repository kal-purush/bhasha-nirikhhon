using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PropPlaceback : MonoBehaviour
{
    [Tooltip("Reference to the variables script")]
    [SerializeField] private PropPlaceBackVariables propPlaceBackVariables; // Reference to the variables script
    [Tooltip("Material of which the alpha is changed once the Target object is in place")]
    [SerializeField] public Material SeeThroughMaterial; // Material of which the alpha is changed once the Target object is in place
    [Tooltip("The time it takes to fade that alpha")]
    [SerializeField] private float fadeTime = 0.1f; // The time it takes to fade that alpha
    [Range(0f, 0.5f)] private float AlphaValue;

    private void Start()
    {
        SeeThroughMaterial.SetFloat("_Alpha", 0.5f);
        AlphaValue = SeeThroughMaterial.GetFloat("_Alpha");
    }

    private void OnTriggerStay(Collider other)
    {
        if (other.gameObject == propPlaceBackVariables.TargetObject)
        {
            propPlaceBackVariables.IsInPlace = true;
            Debug.Log(propPlaceBackVariables.TargetObject.name + " has entered the trigger.");
            StartCoroutine(FadeOut());
        }
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.gameObject == propPlaceBackVariables.TargetObject)
        {
            propPlaceBackVariables.IsInPlace = false;
            Debug.Log(propPlaceBackVariables.TargetObject.name + " has exited the trigger.");
            StartCoroutine(FadeIn());
        }
    }
    
    IEnumerator FadeOut()
    {
        while (SeeThroughMaterial.GetFloat("_Alpha") > 0f)
        {
            AlphaValue -= (fadeTime * Time.deltaTime);
            SeeThroughMaterial.SetFloat("_Alpha", AlphaValue);
            yield return new WaitForEndOfFrame();
        }
    }
    
    IEnumerator FadeIn()
    {
        while (SeeThroughMaterial.GetFloat("_Alpha") < 0.5f)
        {
            AlphaValue += (fadeTime * Time.deltaTime);
            SeeThroughMaterial.SetFloat("_Alpha", AlphaValue);
            yield return new WaitForEndOfFrame();
        }
    }
}