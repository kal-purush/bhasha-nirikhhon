using TMPro;
#if UNITY_EDITOR
using UnityEditor;
#endif
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class MenuUIHandler : MonoBehaviour
{
    public TextMeshProUGUI playerNameInput;
    public TextMeshProUGUI scoreText;


    private void Start()
    {
       scoreText.text= GetData();
    }

    public void OnNameInput()
    {
        DataManager.Instance.playerName=playerNameInput.text;
    }

    private string GetData()
    {
        string text ="Best Score :"+ DataManager.Instance.playerName+" : "+DataManager.Instance.highscore;
        return text;
    }


    public void StartGame()
    {
        SceneManager.LoadScene(1);
    }

    public void QuitGame()
    {
#if UNITY_EDITOR
        EditorApplication.ExitPlaymode();
#else
        Application.Quit();
#endif
    }

}


    
