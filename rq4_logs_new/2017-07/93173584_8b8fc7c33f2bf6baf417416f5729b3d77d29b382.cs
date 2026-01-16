using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HealthBar : MonoBehaviour {

    public GameObject bar;
    public float FullHealth;

    public float displayTime = 2f;
    public float remainTime = -1;

    private float maxHealthWidth = 3f;
//    private float minHealthWidth = 0f;

    public void Start()
    {
        gameObject.SetActive(false);
    }

    public void Update()
    {
        if (remainTime > 0)
        {
            remainTime -= Time.deltaTime;
        }
        else
        {
            gameObject.SetActive(false);
        }
    }

    public void updateHealthBar(float hp)
    {
        if (!gameObject.activeSelf)
        {
            gameObject.SetActive(true);
            remainTime = displayTime;
        }
        float x = (FullHealth - hp) / FullHealth;
        float w = maxHealthWidth * x;

        Vector3 scale = transform.localScale;
        scale.x = w;
        bar.transform.localScale = scale;
    }
	
}