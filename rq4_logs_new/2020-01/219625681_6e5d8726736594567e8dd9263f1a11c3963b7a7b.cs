using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class XOrShow : MonoBehaviour
{
    public GameObject[] objs = null;
    private Image[] imgs = null;
    // Start is called before the first frame update
    void Start()
    {
        imgs = new Image[objs.Length];
        for (int i = 0; i < objs.Length; i++)
        {
            imgs[i] = objs[i].GetComponent<Image>(); 
        }
    }

    public void Show(int index)
    {
        HideAll();
        imgs[index].color = Color.white;
    }

    void HideAll()
    {
        foreach (Image img in imgs)
        {
            img.color = Color.clear;
        }
    }
}