using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class InventoryText : MonoBehaviour {

    public Text foodText;
    public Text fireStoneText;
    public Text waterStoneText;
    public Text electricStoneText;

    public void Start() {
        
        foodText.text = PlayerPrefs.GetInt("Food").ToString();
        fireStoneText.text = PlayerPrefs.GetInt("Fire").ToString();
        waterStoneText.text = PlayerPrefs.GetInt("Water").ToString();
        electricStoneText.text = PlayerPrefs.GetInt("Electric").ToString();
    
    }

    public void Update() {

        foodText.text = PlayerPrefs.GetInt("Food").ToString();
        fireStoneText.text = PlayerPrefs.GetInt("Fire").ToString();
        waterStoneText.text = PlayerPrefs.GetInt("Water").ToString();
        electricStoneText.text = PlayerPrefs.GetInt("Electric").ToString();

    }

}