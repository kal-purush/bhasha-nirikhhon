using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SavePrefs : MonoBehaviour
{
    
    public int coinsToSave;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    public void SaveGame()
    {
        PlayerPrefs.SetInt("SavedInteger", coinsToSave);
        PlayerPrefs.Save();
        Debug.Log("Game data saved!");
    }
    public void LoadGame()
    {
        if (PlayerPrefs.HasKey("SavedInteger"))
        {
            coinsToSave = PlayerPrefs.GetInt("SavedInteger");
            Debug.Log("Game data loaded!");
        }
        else
            Debug.LogError("There is no save data!");
    }

}