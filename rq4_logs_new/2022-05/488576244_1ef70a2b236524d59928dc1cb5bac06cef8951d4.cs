using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameManager : MonoBehaviour
{
    public GameObject hungerText;
    public GameObject happinessText;
    public GameObject robot;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        happinessText.GetComponent<Text>().text = robot.GetComponent<Robot>().happiness.ToString();
        hungerText.GetComponent<Text>().text = robot.GetComponent<Robot>().hunger.ToString();
    }
}