using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using UnityEngine;

public class CmdConsoleLarge : MonoBehaviour
{
    private CmdConsoleManagement cmdConsoleManagement;
    private UnityEngine.UI.Text textBox;

    private List<String> culledText = new List<String>();

    private void Awake()
    {
        cmdConsoleManagement = GameObject.Find("CommandConsole").GetComponent<CmdConsoleManagement>();
        textBox = GameObject.Find("CommandConsole/CmdConsole_lrg/Scroll View/ViewPort/Content/Text").GetComponent<UnityEngine.UI.Text>();
    }

    public void closeConsole()
    {
        this.GetComponentInChildren<UnityEngine.UI.InputField>().text = "";
        GameObject.Find("CommandConsole").GetComponent<Canvas>().enabled = false;
        this.gameObject.SetActive(false);//Must be the last thing ran
    }

    private void changeCulledText()
    {
        if (this.GetComponentInChildren<UnityEngine.UI.Scrollbar>() == null)
        {
            return;
        }

        float value = this.GetComponentInChildren<UnityEngine.UI.Scrollbar>().value;//value is 0 if it is at the very bottom
        int lines = cmdConsoleManagement.getLrgCmdOutput().ToArray().Length;

        
    }

    private void Update()
    {
        //changeCulledText(); //TODO Do something here
        textBox.text = String.Join(String.Empty, cmdConsoleManagement.getLrgCmdOutput().ToArray());

        //Check if the text box is too big
        if (textBox.text.Length > 14000)
        {
            cmdConsoleManagement.trimLargeLogs();
        }

        //Remove any new lines from the input field
        this.GetComponentInChildren<UnityEngine.UI.InputField>().text = Regex.Replace(this.GetComponentInChildren<UnityEngine.UI.InputField>().text, @"\t|\n|\r|`", String.Empty);
    }
}