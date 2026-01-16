using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class InfoPanelUI : PanelBase
{
    private TextMeshProUGUI[] infoTexts;

    private void Start()
    {
        infoTexts = GetComponentsInChildren<TextMeshProUGUI>();
        this.gameObject.SetActive(false);
    }

    public void ShowInfoPanel(string superscription, string virusName, string info, string postScriptum)
    {
        infoTexts[0].text = superscription;
        infoTexts[1].text = virusName;
        infoTexts[2].text = info;
        infoTexts[3].text = postScriptum;

        ChangeVisibility(true);
    }
}