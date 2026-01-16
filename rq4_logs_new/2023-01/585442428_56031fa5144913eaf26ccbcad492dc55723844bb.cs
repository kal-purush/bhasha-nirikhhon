using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Only2ditem : MonoBehaviour
{
    public Texture2D image;
    public GameObject cubelet;

    private void Start()
    {
        int width = image.width;
        int height = image.height;
        var pixels = image.GetPixels();
        float widthCell = transform.localScale.x / width;
        float heightCell = transform.localScale.y / height;

        for (int h = 0; h < height; h++)
        {
            for (int w = 0; w < height; w++)
            {
                int index = h * width + w;
                if (pixels[index].a < 0.5f)
                {
                    continue;
                }
                float wPos = widthCell * w - widthCell / 2 + transform.position.x;
                float hPos = heightCell * h - heightCell / 2 + transform.position.y;
                GameObject cube = Instantiate(cubelet, new Vector3(wPos, hPos, transform.position.z), Quaternion.identity);
                cube.transform.SetParent(transform);
                cube.GetComponent<ChangeColor>().color = pixels[index];
                cube.transform.localScale = new Vector3(widthCell, heightCell, Mathf.Min(widthCell, heightCell));
            }
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}