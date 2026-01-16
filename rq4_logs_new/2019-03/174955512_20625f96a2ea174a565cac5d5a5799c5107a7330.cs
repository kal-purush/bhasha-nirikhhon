using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HexPairObj : MonoBehaviour
{
    [SerializeField]
    GameObject main;
    [SerializeField]
    GameObject sub;
    Hex2Img mainImg, subImg;

    private void Init(){
        mainImg = main.GetComponent<Hex2Img>();
        subImg = sub.GetComponent<Hex2Img>();
    }

    public void ChangeHex(HexColorPair pair){
        if(mainImg == null) Init();
        mainImg.ChangeImage(pair.Main);
        subImg.ChangeImage(pair.Sub);
    }
}