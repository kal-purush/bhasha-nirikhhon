using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class PopUpManager : MonoBehaviour 
{
	public PageProp[] pages;
	public GameObject shadow;

	public TextMesh hintsTitleText, currentHintText, newHintText;
	public Button buttonPrev, buttonNext;

	private int currentHintNum = 0;
	private List<string> hints;

	public static PopUpManager Instance;

	public void Start()
	{
		hints = new List<string>();
		Instance = this;
	}
	public void OpenPage(PageType pagType)
	{
		Time.timeScale = 0;
		foreach (var page in pages)
			page.pageObject.SetActive(pagType == page.pageType);
		shadow.SetActive(true);
	}
	public void CloseWindow()
	{
		foreach (var page in pages)
			page.pageObject.SetActive(false);
		shadow.SetActive(false);
		Time.timeScale = 1;
	}

	public void ShowNewHint(string hintText)
	{
		newHintText.text = hintText;
	}

	public void UpdateTips()
	{
		hints = MainController.Instance.GetHints();
		currentHintNum = 0;

		hintsTitleText.text = "You have " + hints.Count + " hints:";
		if (hints.Count == 0)
			currentHintText.text = "You have no hints";
		else
			currentHintText.text = hints[currentHintNum];
		UpdateButtons();
	}

	public void ShowNextHint(bool next)
	{
		currentHintNum += next ? 1 : -1;
		currentHintText.text = hints[currentHintNum];
		UpdateButtons();
	}

	private void UpdateButtons()
	{
		buttonPrev.gameObject.SetActive(hints.Count > 1 && currentHintNum > 0);
		buttonNext.gameObject.SetActive(hints.Count > 1 && currentHintNum < hints.Count - 1);
	}
}

public enum PageType {NewHint, HintList, Menu}
[System.Serializable]
public class PageProp
{
	public PageType pageType;
	public GameObject pageObject;
}