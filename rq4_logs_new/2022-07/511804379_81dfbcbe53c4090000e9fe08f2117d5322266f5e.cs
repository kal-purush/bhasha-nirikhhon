using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class AccountDataWindowsBase : MonoBehaviour
{
    [SerializeField] private InputField _usernameInputField;
    [SerializeField] private InputField _passwordInputField;

    protected string _username;
    protected string _password;

    private void Start()
    {
        SubscribeElementsUI();
    }

    protected virtual void SubscribeElementsUI()
    {
        _usernameInputField.onValueChanged.AddListener(UpdateUserName);
        _passwordInputField.onValueChanged.AddListener(UpdatePassword);
    }

    protected void EnterInGameScene()
    {
        SceneManager.LoadScene(1);
    }
    private void UpdateUserName(string userName)
    {
        _username = userName;
    }

    private void UpdatePassword(string password)
    {
        _password = password;
    }
}