using UnityEngine;
using System.Collections;
using System;
using UnityEngine.UI;

public class PopUp : MonoBehaviour {

    public static PopUp instance;
    public GameObject[] popUpButtons;
    public GameObject popUpPanel, popUpTitle;

    void Awake() {
        instance = this;
    }

	// Use this for initialization
	void Start () {
        resetButtons();
    }
	
	// Update is called once per frame
	void Update () {
        
    }

    public void showPopUp(string title, string[] buttonTitles, Action[] actionWhenButtonHit) {

        UnityEngine.Assertions.Assert.IsFalse(
            buttonTitles.Length == 0,
            "No button titles provided");
        UnityEngine.Assertions.Assert.IsFalse(
            buttonTitles.Length != actionWhenButtonHit.Length,
            "Button and action array length must be the same");

        resetButtons();

        popUpTitle.GetComponent<Text>().text = title;
        int i;
        for (i = 0 ; i < buttonTitles.Length ; i++) {
            popUpButtons[i].SetActive(true);
            popUpButtons[i].GetComponentInChildren<Text>().text = buttonTitles[i];
            UnityEngine.Events.UnityAction action = new UnityEngine.Events.UnityAction(actionWhenButtonHit[i]);
            popUpButtons[i].GetComponent<Button>().onClick.AddListener(action);
        }
        while(i < popUpButtons.Length) {
            popUpButtons[i].SetActive(false);
            i++;
        }
        popUpPanel.SetActive(true);
    }

    void resetButtons() {
        UnityEngine.Events.UnityAction action = new UnityEngine.Events.UnityAction(() => {
            popUpPanel.SetActive(false);
        });
        for (int i = 0 ; i < popUpButtons.Length ; i++) {
            popUpButtons[i].GetComponent<Button>().onClick.RemoveAllListeners();
            popUpButtons[i].GetComponent<Button>().onClick.AddListener(action);
        }
    }
}