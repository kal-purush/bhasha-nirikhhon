using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class InputControl : MonoBehaviour
{
    [SerializeField] private TMP_InputField inputField;
    [SerializeField] private GameObject caret;
    [SerializeField] private LogControl log;
    private bool canType = true;

    private void Start()
    {
        caret.SetActive(false);
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.Equals))
        {
            canType = !canType;
            inputField.enabled = canType;

            if (canType)
            {
                caret.SetActive(true);
                inputField.ActivateInputField();
            }
            else
            {
                caret.SetActive(false);
                inputField.DeactivateInputField();
                inputField.text = string.Empty;
            }
        }
    }

    public void ReadStringInput(string s)
    {
        if (inputField.text != string.Empty)
        {
            log.AddLog(s);
        }
    }
}

/*
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class InputControl : MonoBehaviour
{
    private string input;
    [SerializeField]
    private TMP_InputField inputField;

    private void Start()
    {
        if (inputField != null)
        {
            inputField.interactable = true;
            inputField.Select();
        }
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.Return))
        {
            ReadInput();
        }
    }

    public void ReadStringInput(string s)
    {
        input = s;
        Debug.Log(s);
    }

    private void ReadInput()
    {
        if (inputField.text != string.Empty && inputField.isFocused)
        {
            ReadStringInput(input);
            inputField.text = string.Empty;
        }
    }
}
*/