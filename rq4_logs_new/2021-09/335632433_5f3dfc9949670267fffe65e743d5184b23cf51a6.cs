using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerHPColor : MonoBehaviour
{
    Slider slider;

    public Image hpColor;
    [SerializeField]
    float hpValueMax;
    [SerializeField]
    float hpValue;
    public float yellowBorder;
    public float redBorder;
    public Color greenColor;
    public Color yellowColor;
    public Color redColor;

    // Start is called before the first frame update
    void Start()
    {
        slider = gameObject.GetComponent<Slider>();
        hpValueMax = slider.value;
    }

    // Update is called once per frame
    void Update()
    {
        hpValue = gameObject.GetComponent<Slider>().value;
        if (hpValue < hpValueMax * (redBorder / 100))
        {
            hpColor.color = redColor;
        }
        else if (hpValue < hpValueMax * (yellowBorder / 100))
        {
            hpColor.color = yellowColor;
        }
        else
        {
            hpColor.color = greenColor;
        }
    }
}