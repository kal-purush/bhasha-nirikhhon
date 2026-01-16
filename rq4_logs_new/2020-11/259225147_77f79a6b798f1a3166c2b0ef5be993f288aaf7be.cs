using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SinWaver : MonoBehaviour
{
    float time = 0;
    int f_10s = 20;
    float h_max = 0.5f;
    float beforeHeight = 0;

    void Start()
    {
        time = 0;
    }

    // Update is called once per frame
    void Update()
    {
        if (time >= 1000f)
        {
            time -= 1000f;
        }
        time += Time.deltaTime;
    }

    public static SinWaver Summon(int f,float h,GameObject parent)
    {
        var instance = parent.AddComponent<SinWaver>();
        instance.SetParamator(f, h);

        return instance;
    }

    public void SetParamator(int f,float h)
    {
        f_10s = f;
        h_max = h;
    }

    public float GetDeltaHeight()
    {
        float h = Mathf.Sin(Mathf.PI * time * (f_10s / 10)) * h_max;
        float result = h - beforeHeight;
        beforeHeight = h;

        return result;
    }
}