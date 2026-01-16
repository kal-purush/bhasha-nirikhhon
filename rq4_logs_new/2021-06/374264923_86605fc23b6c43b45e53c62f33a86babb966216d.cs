using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class FuelBar : MonoBehaviour
{

    public Slider slider;
    public Gradient gradient;
    public Image fill;
    public FuelCapacity fuelCapacity;

    void start()
    {
        slider.maxValue = fuelCapacity.fuel;
        slider.value = fuelCapacity.fuel;

        fill.color = gradient.Evaluate(1f);
    }

    void FixedUpdate()
    {
        slider.value = fuelCapacity.fuel;

        fill.color = gradient.Evaluate(slider.normalizedValue);
    }

}