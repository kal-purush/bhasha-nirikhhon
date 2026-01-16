using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ClickHandler : MonoBehaviour
{
    public bool StartTest = false;
    void Start()
    {
        Button btn = this.GetComponent<Button>();
        btn.onClick.AddListener(OnClick);
    }

    private void OnClick()
    {
        Debug.Log("Button Clicked. ClickHandler.");
        StartTest = true;
    }

    public bool CheckStart() { return StartTest; }

    public void Reset() { StartTest = false; }

}