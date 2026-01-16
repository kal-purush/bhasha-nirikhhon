using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DataInitializer : MonoBehaviour
{
    public DetachablePartsConfig partsConfig;

    void Start()
    {
        InitializeComponents(partsConfig.headComponents);

        InitializeComponents(partsConfig.eyeComponents);

        InitializeComponents(partsConfig.torsoComponents);

        InitializeComponents(partsConfig.rightArmComponents);

        InitializeComponents(partsConfig.leftArmComponents);

        InitializeComponents(partsConfig.rightLegComponents);

        InitializeComponents(partsConfig.leftLegComponents);
    }


    void InitializeComponents(List<DetachablePartsConfig.PartConfig> partConfigs)
    {
        //Initialize data from the DetachablePartsConfig scriptable object
        foreach (var partConfig in partConfigs)
        {
            GameObject sceneObject = GameObject.Find(partConfig.componentName);
            if (sceneObject != null)
            {
                partConfig.gameObject = sceneObject;
                partConfig.meshRenderer = sceneObject.GetComponent<MeshRenderer>();
            }
        }
    }
}