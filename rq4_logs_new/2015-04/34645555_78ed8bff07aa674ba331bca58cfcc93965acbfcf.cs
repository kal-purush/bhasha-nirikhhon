using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using DG.Tweening;

public class CustomizeAvatarPanel : menuPanel {

	private animationController avatar;
	private GameObject toggles;
	public Sprite[] vestsOptionsAvatar;
	public Sprite[] pantsOptionsAvatar;
	public Sprite[] hatsOptionsAvatar;
	public Sprite[] gunsOptionsAvatar;

	private int CurrentHatIndex;
	private int CurrentVestIndex;
	private int CurrenGunIndex;
	private int CurrentPantsIndex;

	GameObject optionpanelsound;
	// Use this for initialization
	void Start () {
		base.Start ();
		toggles = GameObject.Find ("Toggles");
		toggles.SetActive (false);

		vestsOptionsAvatar = Resources.LoadAll<Sprite> ("Vests");
		pantsOptionsAvatar = Resources.LoadAll<Sprite> ("Pants");
		hatsOptionsAvatar = Resources.LoadAll<Sprite> ("Hats");
		gunsOptionsAvatar = Resources.LoadAll<Sprite> ("Guns");

		if (PlayerPrefs.GetInt ("Hat")==null)
			PlayerPrefs.SetInt ("Hat", 0);
		CurrentHatIndex = PlayerPrefs.GetInt ("Hat");
		if (PlayerPrefs.GetInt ("Vest")==null)
			PlayerPrefs.SetInt ("Vest", 0);
		CurrentVestIndex = PlayerPrefs.GetInt ("Vest");
		if (PlayerPrefs.GetInt ("Gun")==null)
			PlayerPrefs.SetInt ("Gun", 0);
		CurrenGunIndex = PlayerPrefs.GetInt ("Gun");
		if (PlayerPrefs.GetInt ("Pants")==null)
			PlayerPrefs.SetInt ("Pants", 0);
		CurrentPantsIndex = PlayerPrefs.GetInt ("Pants");

		avatar = GameObject.Find("Player1").GetComponent<animationController>();
		 
		optionpanelsound = GameObject.Find ("OptionsPanel");
	}
	protected override void ProcessButtonPress(ButtonAction btn)
	{
		switch (btn) {
		case ButtonAction.returnToMain:
		

			uiController.instance.ShowPanel (uiController.instance.MainPanel);
			
			
		
			break;
		case ButtonAction.options:
		
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			uiController.instance.ShowPanel(uiController.instance.OptionsPanel);
			
		
			break;
		}
	}
	public void returnBackToProfile() {

		ProcessButtonPress(ButtonAction.options);
	}

	public void UpdateHatPlus1()
	{
	
		if (CurrentHatIndex + 1 < hatsOptionsAvatar.Length) {
			optionpanelsound.GetComponent<AudioSource>().Play();
			avatar.setHat (CurrentHatIndex + 1);
			CurrentHatIndex += 1;
		}

	}
	public void UpdateHatMinus1()
	{
		if (CurrentHatIndex - 1 >= 0) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setHat (CurrentHatIndex - 1);
			CurrentHatIndex -= 1;
		}
	}
	public void UpdateVestPlus1()
	{
		if (CurrentVestIndex + 1 < vestsOptionsAvatar.Length) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setShirt (CurrentVestIndex + 1);
			CurrentVestIndex += 1;
		}
	}
	public void UpdateVestMinus1()
	{
		if (CurrentVestIndex - 1 >= 0) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setShirt (CurrentVestIndex - 1);
			CurrentVestIndex -= 1;
		}
	}
	public void UpdataeGunPlus1()
	{
		if (CurrenGunIndex + 1 < gunsOptionsAvatar.Length) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setGuns (CurrenGunIndex + 1);
			CurrenGunIndex += 1;
		}
	}
	public void UpdateGunMinus1()
	{
		if (CurrenGunIndex - 1 >= 0) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setGuns (CurrenGunIndex - 1);
			CurrenGunIndex -= 1;
		}
	}
	public void UpdatePantsPlus1()
	{
		if (CurrentPantsIndex + 1 < pantsOptionsAvatar.Length) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setLegs (CurrentPantsIndex + 1);
			CurrentPantsIndex += 1;
		}
	}
	public void UpdatePantsMinus1()
	{
		if (CurrentPantsIndex - 1 >= 0) {
			optionpanelsound.GetComponent<AudioSource> ().Play ();
			avatar.setLegs (CurrentPantsIndex - 1);
			CurrentPantsIndex -= 1;
		}
	}
	public override void TransitionIn()
	{	
		CurrentHatIndex = PlayerPrefs.GetInt ("Hat");
		CurrentVestIndex = PlayerPrefs.GetInt ("Vest");
		CurrenGunIndex = PlayerPrefs.GetInt ("Gun");
		CurrentPantsIndex = PlayerPrefs.GetInt ("Pants");

		toggles.SetActive (true);
		//GameObject.Find ("hat0btn").SetActive (true);
		base.TransitionIn();
	}
	public override void TransitionOut()
	{
		PlayerPrefs.SetInt ("Hat", CurrentHatIndex);
		PlayerPrefs.SetInt ("Vest", CurrentVestIndex);
		PlayerPrefs.SetInt ("Gun", CurrenGunIndex);
		PlayerPrefs.SetInt ("Pants", CurrentPantsIndex);

		toggles.SetActive (false);
		//GameObject.Find ("hat0btn").SetActive (false);
		base.TransitionOut ();
	}

	public static int[] GetSpriteArray()
	{
		int [] CurrentSpriteArray = {PlayerPrefs.GetInt("Hats"),PlayerPrefs.GetInt("Vest"),PlayerPrefs.GetInt("Gun"),PlayerPrefs.GetInt("Pants")  };
		return CurrentSpriteArray;
	}

}