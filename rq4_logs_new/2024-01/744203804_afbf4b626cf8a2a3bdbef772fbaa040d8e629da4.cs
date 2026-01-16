using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ActiveSlot : MonoBehaviour
{
    public Image icon;
    public Image cooldownIndicator;
    public float totalCooldown;
    public float currentCooldown;
    public bool isSet = false;
    // Start is called before the first frame update
    void Start()
    {
        
    }
    void FixedUpdate()
    {
        if (isSet)
        {
            float size = 100f * currentCooldown / totalCooldown;
            cooldownIndicator.rectTransform.sizeDelta = new Vector2 (100f, size);
            cooldownIndicator.rectTransform.anchoredPosition = new Vector2(cooldownIndicator.rectTransform.anchoredPosition.x, (size - 100f) / 2f);
        }
    }
}