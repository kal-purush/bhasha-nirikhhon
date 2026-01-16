using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class UserInfoPanelManager : MonoBehaviour {
	[SerializeField] Image userInfoPanelOfRecruit;
	[SerializeField] Image userInfoPanelOfTraining;
	public static UserInfoPanelManager instance;
	void Start ()
	{
		// singleton化する
		if (instance == null)
		{
			instance = this;
			DontDestroyOnLoad (instance.gameObject);
		}
	}

	public void ResetUserInfo ()
	{
		 userInfoPanelOfRecruit.GetComponent<UserInfoPanelBehavior>().SetValues();
		 userInfoPanelOfTraining.GetComponent<UserInfoPanelBehavior>().SetValues();
	}
}