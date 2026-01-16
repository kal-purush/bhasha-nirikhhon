using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ResourceUI : MonoBehaviour
{
    [SerializeField]
    private Text textComponent;

    private float timeForResource = 2f;
    private float elapsedTime;

    void Update()
    {
        textComponent.text = ResourceType.WOOD + ": " + ResourceManager.GetAmount(ResourceType.WOOD);

        elapsedTime += Time.deltaTime;
        if (elapsedTime >= timeForResource) {
            ResourceManager.AddResource(ResourceType.WOOD, 1);
            elapsedTime = 0;
        }
    }
}