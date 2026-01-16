using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class DisplayScore : MonoBehaviour
{
    Text score;
    private float turns;



	// Use this for initialization
	void Start () 
    {
        for (int x = 0; x < 3; x++)
        {
            turns = Random.Range(1, 5);
            score = gameObject.GetComponent<Text>();
            score.text = "" + turns;
        }
	}
	
	// Update is called once per frame
	void Update ()
    {
        //score.text = "0";	
	}
}