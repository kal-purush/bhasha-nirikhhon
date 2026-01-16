using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class UpgradeController : MonoBehaviour {
	
	public Base _base;
	public Text upgradePoints;
 
	// Use this for initialization
	void Start (){
		
		if (SaveManager.Instance.GetSkillLevel (SkillType.Hammer) != 0) {
			SaveManager.Instance.GetSkillLevel (SkillType.Hammer);
		} else {
			SaveManager.Instance.SaveSkillLevel (SkillType.Hammer, 0);
		}

		if (SaveManager.Instance.GetSkillLevel (SkillType.Guitar) != 0) {
			SaveManager.Instance.GetSkillLevel (SkillType.Guitar);
		} else {
			SaveManager.Instance.SaveSkillLevel (SkillType.Guitar, 0);
		}

		if (SaveManager.Instance.GetSkillLevel (SkillType.Curtain) != 0) {
			SaveManager.Instance.GetSkillLevel (SkillType.Curtain);
		} else {
			SaveManager.Instance.SaveSkillLevel (SkillType.Curtain, 0);
		}

		if (PlayerPrefs.HasKey ("points")) {

		} else {
			PlayerPrefs.SetInt ("points", 0);
		}
	
		upgradePoints.text = string.Format("Available: {0}",PlayerPrefs.GetInt("points"));
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}