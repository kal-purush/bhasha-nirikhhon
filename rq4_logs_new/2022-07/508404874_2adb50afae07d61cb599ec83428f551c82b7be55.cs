using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LightSwitch : MonoBehaviour, IInterractiveItem
{
    [SerializeField] private List<Light> lights = new List<Light>();


    public void OnIterraction(GameObject sender)
    {
        foreach(Light light in lights)
        {
            light.enabled = !light.enabled;
        }
    }
}