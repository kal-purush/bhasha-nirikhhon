using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class createCrowed : MonoBehaviour
{
    private GameObject head;
    private GameObject head_position;
    private GameObject clothJacket;
    private GameObject clothShadow;
    private GameObject targetHead;
    private GameObject acc;
    private GameObject[] itemPosition = new GameObject[10];

    public GameObject[] feildObject = new GameObject[7];
    public GameObject[] item = new GameObject[10]; 

    
    

    public GameObject self;
    public GameObject self_prefab;

    
    // Start is called before the first frame update
    void Start()
    {
        head = GameObject.FindGameObjectWithTag("Head_clone");
        acc = GameObject.FindGameObjectWithTag("Acc_clone");
        head_position = GameObject.FindGameObjectWithTag("HeadPosition_clone");
        clothJacket = GameObject.FindGameObjectWithTag("Cloth_jacket_clone");
        clothShadow = GameObject.FindGameObjectWithTag("Cloth_shadow_clone");
        itemPosition[0] = GameObject.FindGameObjectWithTag("acc1");
        itemPosition[1] = GameObject.FindGameObjectWithTag("acc2");
        itemPosition[2] = GameObject.FindGameObjectWithTag("acc3");
        itemPosition[3] = GameObject.FindGameObjectWithTag("acc4");
        itemPosition[4] = GameObject.FindGameObjectWithTag("acc5");
        itemPosition[5] = GameObject.FindGameObjectWithTag("acc6");
        itemPosition[6] = GameObject.FindGameObjectWithTag("acc7");
        itemPosition[7] = GameObject.FindGameObjectWithTag("acc8");
        itemPosition[8] = GameObject.FindGameObjectWithTag("acc9");
        itemPosition[9] = GameObject.FindGameObjectWithTag("acc10");
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public GameObject CreatCrowdCharacter(int index,int field ,float saturation, int year){
        // self = selfPrefab;
 
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
        if(head){
            Destroy(head);
        }
        targetHead = feildObject[field-1];
        mainColor =  Color.HSVToRGB(hue, 1, value);
        head = Instantiate(targetHead);
        head.transform.position=  head_position.transform.position;
        head.transform.rotation =  head_position.transform.rotation;
        head.transform.parent = head_position.transform.parent;
        head.GetComponent<Renderer>().material.color = mainColor;


        if(acc){
            Destroy(acc);
        }
        acc = Instantiate(item[year], itemPosition[year].transform.position, itemPosition[year].transform.rotation);
        acc.transform.parent = itemPosition[year].transform;
        acc.GetComponent<Renderer>().material.color = mainColor;

        clothJacket.GetComponent<Renderer>().material.color = Color.HSVToRGB(hue,saturation,value);
        clothShadow.GetComponent<Renderer>().material.color = mainColor;

        return Instantiate(self);










        
        
    }
}