using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Highscore : MonoBehaviour
{
    public Text firstPlace;
    public Text secondPlace;
    public Text thirdPlace;
    public Text fourthPlace;
    public Text fifthPlace;
    public Text sixthPlace;
    public Text seventhPlace;
    public Text eighthPlace;
    public Text ninthPlace;
    public Text tenthPlace;

    // Use this for initialization
    void Start ()
    {
        firstPlace.text="1. "+ PlayerPrefs.GetInt("0HighScore").ToString();
        secondPlace.text="2. "+ PlayerPrefs.GetInt("1HighScore").ToString();
        thirdPlace.text = "3. " + PlayerPrefs.GetInt("2HighScore").ToString();
        fourthPlace.text = "4. " + PlayerPrefs.GetInt("3HighScore").ToString();
        fifthPlace.text = "5. " + PlayerPrefs.GetInt("4HighScore").ToString();
        sixthPlace.text = "6. " + PlayerPrefs.GetInt("5HighScore").ToString();
        seventhPlace.text = "7. " + PlayerPrefs.GetInt("6HighScore").ToString();
        eighthPlace.text = "8. " + PlayerPrefs.GetInt("7HighScore").ToString();
        ninthPlace.text = "9. " + PlayerPrefs.GetInt("8HighScore").ToString();
        tenthPlace.text = "10. " + PlayerPrefs.GetInt("9HighScore").ToString();
    }
	
	// Update is called once per frame
	void Update ()
    {
		
	}
}