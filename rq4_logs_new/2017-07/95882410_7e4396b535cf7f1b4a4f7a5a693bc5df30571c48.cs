using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;

public class Answer : MonoBehaviour {

	//the text of answer
	public string answerText;

	//list of changes
	public Dictionary<string,string> settings;

	//settings array
	public string[] settingsArr;

	//tag which defines the type of parametr
	public string parametrTag;

	//defines if answer will repeats in future
	public bool repeat;

	//defines if answer can be used only one time
	public bool oneTimeUse;

	//defines if answer is forbidden to use
	public bool used;

	//DRAG & DROP obj here
	public WorldController wcont;

	// Use this for initialization
	void Start () {
		this.GetComponentInChildren<Text> ().text = answerText;
	}
	
	public void OnClick(){
		/*foreach (var item in settings) {
			wcont.UpdateGameSetting (item.Key, item.Value);
		}*/
		for (int i = 0; (i < Mathf.FloorToInt (settingsArr.Length / 2));i += 2) {
			wcont.UpdateGameSetting (settingsArr[i], settingsArr[i+1]);
		}

		Destroy (this.transform.parent.gameObject);
	}
}