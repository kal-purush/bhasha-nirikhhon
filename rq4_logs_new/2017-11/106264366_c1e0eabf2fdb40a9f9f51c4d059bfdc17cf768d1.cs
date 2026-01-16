using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BackgroundScript : MonoBehaviour
{
    float yMax = 10f;
    float xMax = 17.778f;

    private float x;
    private float y;

    public static BackgroundScript Instance;
    public void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
        }
        else
        {
            Destroy(gameObject);
        }
    }

    private void Start()
    {
        x = xMax * SizeCoefInGame.Instance.GetSizeCoeff();
        //x = x / 3.55f;
        gameObject.transform.localScale = new Vector3(x, yMax, 0);
    }
}