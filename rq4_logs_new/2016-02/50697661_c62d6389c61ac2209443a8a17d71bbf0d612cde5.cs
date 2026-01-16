using UnityEngine;
using System.Collections;

public class SaveDataManager : MonoBehaviour {

	public static SaveDataManager instance_;
	private int bestLevel;

	// Use this for initialization
	void Awake () {
		instance_=this;
		if(PlayerPrefs.GetInt("bestLevel")==null){
			bestLevel=1;
			PlayerPrefs.SetInt("bestLevel",bestLevel);
		}else{
			PlayerPrefs.GetInt("bestLevel");
		}
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public void setBestLevel(int level){
		if(level>bestLevel){
			bestLevel=level;
			PlayerPrefs.SetInt("bestLevel",bestLevel);
		}
	}
}