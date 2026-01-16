using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class SkillDescription : MonoBehaviour {
	public Text desText;
	public string SkillName;
	public Base _base;
	// Use this for initialization

	void Start () {

		switch(SkillName)
		{
		case "guitar":
			this.desText.text = string.Format("Дає можливість на певний час приспати ворогів. Зараз: {0} секунд, наступний рівень {1} секунд",_base.skills[0].powerValue[PlayerPrefs.GetInt("guitarLVL")],_base.skills[0].powerValue[PlayerPrefs.GetInt("guitarLVL")+1]);
			break;
		case "hummer":
			this.desText.text = string.Format("Наносить певну кількість шкоди ворогам. Зараз: {0} одиниць шкоди, наступний рівень {1} одиниць шкоди",_base.skills[1].powerValue[PlayerPrefs.GetInt("hummerLVL")],_base.skills[1].powerValue[PlayerPrefs.GetInt("hummerLVL")+1]);
			break;
		case "shower":
			this.desText.text = string.Format("Робить Афанасія невидимим для ворогів на певний час. Зараз: {0} секунд, наступний рівень {1} секунд",_base.skills[2].powerValue[PlayerPrefs.GetInt("showerLVL")],_base.skills[2].powerValue[PlayerPrefs.GetInt("showerLVL")+1]);
			break;
		default:
			break;
		}
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}