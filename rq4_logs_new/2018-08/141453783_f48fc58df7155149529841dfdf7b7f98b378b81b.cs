using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CountBox : MonoBehaviour {
    public int countBox = 0;
    public Text count;
    public string NextSceneString;
    // Use this for initialization
    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        Text t = count.GetComponent<Text>();

        switch (countBox)
        {
            case 0:
                t.text = "( 0 / 5 )";
                break;
            case 1:
                t.text = "( 1 / 5 )";
                break;
            case 2:
                t.text = "( 2 / 5 )";
                break;
            case 3:
                t.text = "( 3 / 5 )";
                break;
            case 4:
                t.text = "( 4 / 5 )";
                break;
            case 5:
                t.text = "( 5 / 5 )";
                Application.LoadLevel(NextSceneString);
                break;
        }
        
    }
}