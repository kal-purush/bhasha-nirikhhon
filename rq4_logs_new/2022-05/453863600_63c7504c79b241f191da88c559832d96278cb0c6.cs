using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BatteryLevelHifi : MonoBehaviour
{

    // Update is called once per frame
    void Update()
    {
        
        RectTransform rect = gameObject.GetComponent<RectTransform>();
        rect.sizeDelta = new Vector2(batteryWidth(), rect.sizeDelta.y);
    }

    float batteryWidth() {
      return SystemInfo.batteryLevel * 76;
    }


}