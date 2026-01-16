using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Hex2Img : MonoBehaviour
{
    Image image;
    [SerializeField]
    Sprite spriteYellow;
    [SerializeField]
    Sprite spriteBlue;
    [SerializeField]
    Sprite spritePurple;
    [SerializeField]
    Sprite spriteBlack;
    [SerializeField]
    Sprite spriteWhite;
    [SerializeField]
    Sprite spriteBlank;

    Sprite[] sprites;
    // Start is called before the first frame update
    void Start()
    {
        image = GetComponent<Image>();
        sprites = new Sprite[]{
            spriteBlank,
            spriteYellow,
            spriteBlue,
            spritePurple,
            spriteBlack,
            spriteWhite
        };
    }

    public void ChangeImage(HexColor color){
        image.sprite = sprites[(int)color];
    }
}