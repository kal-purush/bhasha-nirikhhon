using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class CodexHandler : MonoBehaviour {


    //EVENTS
    public delegate void AnswerCallback(string message);

    // Event declaration
    public event AnswerCallback OnUnityAnswers;


    GameObject textPanel;
    Text codexText;

    int numberOfCodecies;
	// Use this for initialization
	void Start ()
    {
        //get text component on codex text panel
        codexText = textPanel.GetComponent<Text>();
        //make sure text panel is inactive
        textPanel.SetActive(false);
        //numberOfCodecies = 
	
        if(!PlayerPrefs.HasKey("Codecies"))
        {
            //string
           // PlayerPrefs.SetString();
        }
	}
	
	// Update is called once per frame
	void Update () {
	
	}


}