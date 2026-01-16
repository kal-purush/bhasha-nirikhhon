using System.Collections;
using System;
using UnityEngine;
//add using statement for Coroutines

//create a helper class that null checks a static instance is not null in 10 frames and return a bool value with out MonoBehaviour
public static class ComponentNullCheck
{
    public static IEnumerator CheckNullComponent<T>(T component, string componentName, Action<bool> isNull, int iterations) where T : MonoBehaviour
    {
        //checks if the component is null and waits for i frames
        for (int i = 0; i < iterations; i++)
        {
            if (i >= 10)
            {
                Debug.Log(componentName + " was not set by script in 10 frames ", component.transform);
                yield return null;
            }
            if (component == null)
            {
                yield return new WaitForEndOfFrame();
            }
            else
                continue;
        }
        isNull(false);
        Debug.Log("Loaded " + componentName + " Instance to " + component.name);
        yield return null;
    }
}
//how to call
//StartCoroutine(ComponentNullCheck.CheckNullComponent<CinemachineVirtualCamera>(_camera, "VirtualCamera", x => _isCameraNull = x, 10));

