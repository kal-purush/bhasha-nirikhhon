using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_AsteroidIndicator : MonoBehaviour
{
    [Header("Sprite References")]
    [SerializeField] private Sprite sprite1;
    [SerializeField] private Sprite sprite2;
    [SerializeField] private Sprite sprite3;
    [SerializeField] private Sprite sprite4;

    private int randomSprite;

    private void Awake()
    {
        randomSprite = Random.Range(1, 4);

        switch (randomSprite)
        {
            case 1:
                GetComponent<SVGImage>().sprite = sprite1;
                break;
            case 2:
                GetComponent<SVGImage>().sprite = sprite2;
                break;
            case 3:
                GetComponent<SVGImage>().sprite = sprite3;
                break;
            case 4:
                GetComponent<SVGImage>().sprite = sprite4;
                break;
        }
    }
}