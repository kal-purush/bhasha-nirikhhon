using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PatternGrid : MonoBehaviour
{
    public Image image;

    GridLayoutGroup gridLayout;

    public RectTransform rectTransform;

    int imageSize = 100;

    public Sprite corpse;

    public int[,] storedPattern;

    public bool big;


    // Start is called before the first frame update
    void Start()
    {
        gridLayout = GetComponent<GridLayoutGroup>();
        rectTransform = GetComponent<RectTransform>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void setPattern(int[,] pattern)
    {
        rectTransform = GetComponent<RectTransform>();


        pattern = PatternStore.Rotate(pattern);

        storedPattern = pattern;

        foreach (Transform child in transform)
        {
            Destroy(child.gameObject);
        }

        int size = pattern.GetLength(0) > pattern.GetLength(1) ? pattern.GetLength(0) : pattern.GetLength(1);


        for (int i = 0; i < size * size; i++)
        {
            Image newImage = Instantiate(image);
            newImage.transform.parent = this.transform;
            newImage.transform.localScale = Vector3.one;
            newImage.color = new Color(1, 1, 1, 0);

            int x = i / size;
            int y = i % size;

            if (x < pattern.GetLength(0) && y < pattern.GetLength(1))
            {
                int corpseNum = pattern[x, y];
                if (corpseNum != 0 && corpseNum != -1)
                {
                    newImage.sprite = corpse;
                    newImage.color = new Color(1, 1, 1, 1);
                }

            }

        }

        scale();
        if (big)
        {
            makeBig();
        }
        
    }


    public void toggleBig()
    {
        if (!big)
        {
            makeBig();
            big = true;

        }
        else
        {
            makeUnbig();
            big = false;
        }

    }

    private void scale()
    {
        int[,] pattern = storedPattern;

        int size = pattern.GetLength(0) > pattern.GetLength(1) ? pattern.GetLength(0) : pattern.GetLength(1);

        rectTransform.sizeDelta = new Vector2(size * imageSize, size * imageSize);
        float scale = 1f / size;
        rectTransform.localScale = new Vector2(scale, scale);
    }

    private void makeBig()
    {
        rectTransform.localScale *= 3.5f;
        setOpacity( 0.4f);
        setOpacity( 0.4f);
    }

    private void makeUnbig()
    {
        rectTransform.localScale = Vector3.one;
        scale();
        setOpacity( 1f);
        setOpacity( 1f);
    }

    private void setOpacity( float opacity)
    {

        foreach (Transform child in transform)
        {
            GameObject obj = child.gameObject;
            if (obj == null)
            {
                continue;
            }
            // Get the Image component
            Image imageComponent = obj.GetComponent<Image>();

            // Check if the Image component exists
            if (imageComponent != null)
            {
                // Get the current color
                Color currentColor = imageComponent.color;

                // Set the transparency to 50%
                currentColor.a = opacity;

                // Assign the new color with adjusted transparency
                imageComponent.color = currentColor;
            }
            else
            {
                Debug.LogWarning("Image component not found in " + obj.name);
            }
        }
    }
}