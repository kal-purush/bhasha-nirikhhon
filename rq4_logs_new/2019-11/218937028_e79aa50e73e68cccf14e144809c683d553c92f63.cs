using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CreateCrowd : MonoBehaviour
{
    public GameObject clothJacket;
    public GameObject clothShadow;
    public GameObject[] item = new GameObject[10];
    public GameObject[] head = new GameObject[7];

    public void createCrowdCharacter(int field ,float saturation, int year) {
        setInitState();

        float hue = 1;
        float value = 1;
        Color mainColor;

        switch (field)
        {
            case 1 :
                hue = 352f / 360f;
                break;

            case 2 :
                hue = 26f / 360f;
                break;

            case 3 :
                hue = 50f / 360f;
                break;

            case 4 :
                hue = 97f / 360f;
                value = 0.902f;
                break;

            case 5 :
                hue = 188f / 360f;
                value = 0.902f;
                break;

            case 6 :
                hue = 224f / 360f;
                break;

            case 7 :
                hue = 265f / 360f;
                break;  

            default:
                hue = 352f / 360f;
                break;
        }

        mainColor =  Color.HSVToRGB(hue, 1, value);
        head[field - 1].SetActive(true);
        head[field - 1].GetComponent<Renderer>().material.color = mainColor;

        item[year - 1].SetActive(true);
        item[year - 1].GetComponent<Renderer>().material.color = mainColor;

        clothShadow.GetComponent<Renderer>().material.color = mainColor;
        clothJacket.GetComponent<Renderer>().material.color = Color.HSVToRGB(hue, saturation, value);
    }

    private void setInitState() {
        foreach (GameObject eachHead in head) {
            eachHead.SetActive(false);
        }
        foreach (GameObject eachAcc in item) {
            eachAcc.SetActive(false);
        }
    }
}