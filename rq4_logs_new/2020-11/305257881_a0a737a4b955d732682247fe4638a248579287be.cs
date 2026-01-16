using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HealthBar : MonoBehaviour
{
    private Image barImage;
    private void Awake()
    {
        barImage = transform.Find("HPBar").GetComponent<Image>();
    }

    public void SetHealth(float healthNormalized)
    {
        barImage.fillAmount = healthNormalized;
    }

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }
}