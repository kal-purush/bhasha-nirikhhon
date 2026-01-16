using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Generator : MonoBehaviour
{
    
    public GameObject DirtPrefab;

    int minX = -8;
    int maxX = 8;
    int minY = -8;
    int maxY = 8;

    PerlinNoise noise;

    void Start()
    {
        Regenerate();
    }

    private void Regenerate() 
    {
        
        float width = DirtPrefab.transform.lossyScale .x;
        float height = DirtPrefab.transform.lossyScale.y;

        for (int i = minX; i < maxX; i++) //colums (x values)
        {    
            for (int j = minY; j < maxY; j ++) //
            {
                Instantiate(DirtPrefab, new Vector2(i * width, j * height), Quaternion.identity);
            }
        }
    }
   
}