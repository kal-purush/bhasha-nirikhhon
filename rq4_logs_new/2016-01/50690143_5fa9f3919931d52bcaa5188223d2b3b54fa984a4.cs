using UnityEngine;
using System.Collections;

public class ComboSystem : MonoBehaviour {

    // Variables
    public string key = "";
    private float timeUser = 0;
    public float comboLimit;

    // Combo String
    // FUTURE DEVELOPMENT: random generation of keys
    private string raincombo = "LURD";
    private string windCombo = "UURD";
    private string fireCombo = "LDUL";
    private string earthquakeCombo = "UDUD";

    // Use this for initialization
    void Start () {
	
	}

    // Update is called once per frame
    void Update()
    {

        // We check if the time between keys is greater than specific amount
        timeUser += Time.deltaTime;

        // Check if the user has stop input key during a specified time
        if (timeUser > comboLimit)
        {
            key = "";
            timeUser = 0;
        }

        // Check for the Key Input pressed, we check the four arrow keys
        if (Input.GetKeyDown(KeyCode.UpArrow))
        {
            // 
            key += "U";
            timeUser = 0;
        }
        else if (Input.GetKeyDown(KeyCode.DownArrow))
        {
            key += "D";
            timeUser = 0;
        }
        else if (Input.GetKeyDown(KeyCode.LeftArrow))
        {
            key += "L";
            timeUser = 0;
        }
        else if (Input.GetKeyDown(KeyCode.RightArrow))
        {
            key += "R";
            timeUser = 0;
        }
        
        // We check if the accumulated string is the same as element combo
        if (key.Equals(raincombo))
        {
            // TO DO RAIN ANIMATION AND STAFF
        } else if (key.Equals(fireCombo))
        {
            // TO DO FIRE ANIMATION AND STAFF
        }
        else if (key.Equals(windCombo))
        {
            // TO DO RAIN WIND ANIMATION AND STAFF
        }
        else if (key.Equals(earthquakeCombo))
        {
            // TO DO RAIN EARTH QUAKE ANIMATION AND STAFF
        }

    }

}