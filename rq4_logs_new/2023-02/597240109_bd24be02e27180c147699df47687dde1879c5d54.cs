using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class Chronometer : MonoBehaviour
{
    private bool toggle = false;
    public float originalValue;
    public float value;

    TMP_Text time;

    public void OnOff(bool x)
    {
        toggle = x;
    }

    public void SetValue(float newValue) {
        originalValue = newValue;
    }

    public void Reset() {
        value = originalValue;
    }

    void Start() {
        time = GetComponent<TMP_Text>();
    }

    void Update()
    {
        if (toggle) {
            value -= Time.deltaTime;
            time.text = "TIME: " + value.ToString("0.00"); 
        }
    }
}