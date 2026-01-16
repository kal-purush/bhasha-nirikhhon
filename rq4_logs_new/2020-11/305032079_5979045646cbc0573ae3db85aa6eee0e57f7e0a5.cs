using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class SignUp : MonoBehaviour
{
    public TextMeshProUGUI txtEmail;

    public TextMeshProUGUI txtNickname;

    public TextMeshProUGUI txtPassword;

    public TextMeshProUGUI txtPasswordConfirm;

    public TextMeshProUGUI txtConfirmationCode;

    public TextMeshProUGUI txtName;
    
    public TextMeshProUGUI txtLastame;
    
    public void signUp()
    {
        string emailText = txtEmail.text;
        string nicknameText = txtNickname.text;
        string passwordText = txtPassword.text;
        string passwordConfirmText = txtPasswordConfirm.text;
        string nameText = txtName.text;
        string lastameText = txtLastame.text;
        string codeText = txtConfirmationCode.text;

        Debug.Log(emailText);
        Debug.Log(nicknameText);
        Debug.Log(passwordText);
        Debug.Log(passwordConfirmText);
        Debug.Log(nameText);
        Debug.Log(lastameText);
        Debug.Log(codeText);
    }
}