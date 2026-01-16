using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class BottomPanel : MonoBehaviour
{
    public GameObject bottomPanel;
    public Button toggleBottomPanelButton;
    public Text toggleButtonText;

    private Vector3 moveBottomPanelVecotr = new Vector3(0.0f, 100.0f, 0.0f);
    private bool isBottomPanelHidden = false;

    public void ToggleBottomPanel()
    {
        if (isBottomPanelHidden)
        {
            ShowPanel();
        }
        else
        {
            HidePanel();
        }
    }

    private void HidePanel()
    {
        if (isBottomPanelHidden)
            return;

        toggleBottomPanelButton.transform.Translate(-moveBottomPanelVecotr);
        bottomPanel.transform.Translate(-moveBottomPanelVecotr);
        toggleButtonText.text = "^ EXPAND ^";
        isBottomPanelHidden = true;
    }
    private void ShowPanel()
    {
        if (!isBottomPanelHidden)
            return;

        toggleBottomPanelButton.transform.Translate(moveBottomPanelVecotr);
        bottomPanel.transform.Translate(moveBottomPanelVecotr);
        toggleButtonText.text = "v FOLD v";
        isBottomPanelHidden = false;
    }
}