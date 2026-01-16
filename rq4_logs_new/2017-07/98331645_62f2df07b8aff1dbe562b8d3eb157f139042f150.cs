using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class Bars : MonoBehaviour {


    public float howHuchOfBarIsFilled;
    public Image barFillerImage;

    public float lerpSpeed = 3;

    public float maxValue;
    public float currentValue;
    
    public void UpdateBarFill()
    {
        howHuchOfBarIsFilled = Map(currentValue, 0, maxValue, 0, 1);
        //barFillerImage.fillAmount = howHuchOfBarIsFilled;
        barFillerImage.fillAmount = Mathf.Lerp(barFillerImage.fillAmount, howHuchOfBarIsFilled, lerpSpeed);
    }

    private float Map(float value, float inMin, float inMax, float outMin, float outMax)
    {
        //return (value - inMin) * (outMax - outMin) / (inMax - inMin) + outMax;
        return (value - inMin) * (outMax - outMin) / (inMax - inMin); // + outMax naudojamas tik tam kad butu uzpildyti pacioje pradzioje kai dar nepriskiriam verciu
    }

	void Start ()
    {
	
	}

	void Update ()
    {
	
	}
}