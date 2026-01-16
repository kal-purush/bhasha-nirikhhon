using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class CodexPrep : MonoBehaviour {


    //the total number of codecies available in game (the number in resources folder)
    public static int maxCodecies = 37;
    public static Dictionary<int, string> codexTextDict = new Dictionary<int, string>();

    string codexPref;
    int codexIndex = 0;
    // Use this for initialization
    void Start ()
    {
        //TEST CODE TO CLEAR PLAYERPREF
        //PlayerPrefs.DeleteAll();
        
        //Check playerprefs
        if (!PlayerPrefs.HasKey("Codecies"))
        {
            //create a string of all 0's that is the same size as the number of codecies
            string codexString = new string('0', maxCodecies);
            //set the playerpref to have the same data
            PlayerPrefs.SetString("Codecies", codexString);

        }
        codexPref = PlayerPrefs.GetString("Codecies");
        //make sure the playerpref includes all codecies (including new ones)
        if(codexPref.Length < maxCodecies)
        {
            //add 0s for all new codecies
            for(int i = 0; i < (maxCodecies - codexPref.Length); i++)
            {
                codexPref = codexPref + "0";
            }
            //reassign playerpref for next use
            PlayerPrefs.SetString("Codecies", codexPref);
        }

        //create a dictionary for the in-game codecies to reference
        foreach (TextAsset codex in Resources.LoadAll("Codecies"))
        {
            
            codexIndex++;
            //create new entry if key doesnt exist
            if(!codexTextDict.ContainsKey(codexIndex))
            {
                codexTextDict.Add(codexIndex, codex.text);
            }
            //if it does exist, update it in case text has changed
            else
            {
                codexTextDict[codexIndex] = codex.text;
            }
            
          
        }



    }
	
	// Update is called once per frame
	void Update () {
	
	}
}