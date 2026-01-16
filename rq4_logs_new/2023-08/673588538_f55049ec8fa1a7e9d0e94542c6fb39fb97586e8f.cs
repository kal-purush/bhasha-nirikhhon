using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScoreBoardNumber : MonoBehaviour
{
    [SerializeField]
    private int number;
    
    private List<GameObject> upper;
    private List<GameObject> upperLeft;
    private List<GameObject> upperRight;
    private List<GameObject> middle;
    private List<GameObject> middleLeft;
    private List<GameObject> middleRight;
    private List<GameObject> lower;


    // Start is called before the first frame update
    void Start()
    {
        number = 0;

        foreach (Transform child in transform)
        {
            if(child.name.Contains("Upper Light"))
            {
                Light light = child.GetComponentInChildren<Light>();
                light.enabled = false;
            }
        }


    }

    // Update is called once per frame
    void Update()
    {
        // if (Input.GetKeyDown(KeyCode.Space))
        // {
        //     Debug.Log("Number: " + number);
        // } 
        // else if (Input.GetKeyDown(KeyCode.Keypad1))
        // {
        //     SetNumber(1)
        // }


    }

    public void SetNumber(int numberToSet)
    {
        number = numberToSet;
    }
}