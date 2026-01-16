using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class CreatePattern : MonoBehaviour
{
	public TextMeshProUGUI showPattern;
	public Toggle[] toggles = null;
	public List<int> pattern = null;
	public Button btn1;
	void Start()
    {
		btn1 = GetComponent<Button>();
		pattern = new List<int>(25);
		for (int i = 0; i < 26; i++)
		{
			pattern.Insert(i, 0);
		}

		foreach (Toggle toggle in toggles)
		{
			Toggle captured = toggle;
			toggle.onValueChanged.AddListener((value) => ToggleStateChanged(captured, value));
		}
	}

    void Update()
    {
        
    }
	public void OnClick()
    {
		PrintPattern();
    }
	private void ToggleStateChanged(Toggle toggle, bool state)
	{		
		Debug.Log(toggle.GetComponent<Toggle>().name + " is " + state.ToString());
	}

	private void PrintPattern()
	{		
		int count2 = 0;
		string pattern = null;
		while (count2 < 26)
		{
			pattern += "-" + this.	pattern[count2];
			this.showPattern.text = pattern;
			count2++;
		}
	}
}