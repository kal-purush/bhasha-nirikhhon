using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LightSwitch : Interactable
{
    public List<GameObject> lights = new List<GameObject>();
    public override void Interact()
    {
        foreach (GameObject light in lights)
        {
            light.SetActive(!light.activeSelf);
        }
    }
}