using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Script_CanvasMenu : MonoBehaviour
{
    [SerializeField] Button m_ButtonPlay;
    [SerializeField] Button m_ButtonInfo;
    [SerializeField] Button m_ButtonInput;
    [SerializeField] Button m_ButtonCredits;
    [SerializeField] Button m_ButtonQuit;
    [SerializeField] Text m_BoardText;

    //TODO Handle the following board messages in a proper way 
    private string m_INFO = "Messages info";
    private string m_INPUT = "Input info";
    private string m_CREDITS = "Made with Unity by...";

    private Script_Pause m_script_Pause;

    private bool m_firstExec = true;

    void Awake()
    {
        m_ButtonPlay.onClick.AddListener(Play);

        m_ButtonInfo.onClick.AddListener(() => WriteBoardMessage(m_INFO));
        m_ButtonInput.onClick.AddListener(() => WriteBoardMessage(m_INPUT));
        m_ButtonCredits.onClick.AddListener(() => WriteBoardMessage(m_CREDITS));

        m_ButtonQuit.onClick.AddListener(Quit);

        GameObject rootGame = GameObject.FindWithTag("RootGame");
        m_script_Pause = rootGame.GetComponent<Script_Pause>();
    }

    void Play()
    {
        if (m_firstExec)
        {
            var text = m_ButtonPlay.GetComponentInChildren<Text>();
            text.text = "RESUME!";
            m_firstExec = false;
            m_script_Pause.Execute();
        }
        m_script_Pause.ResumeGame();
    }

    void WriteBoardMessage(string message)
    {
        m_BoardText.text = message;
    }

    void Quit()
    {
        Application.Quit(); // Warning: this only works when the project is built. Won't work when playing in the editor
    }

    /// <summary>
    /// Allows sending generic messages (info, quest, etc) during game play. Game will get paused
    /// </summary>
    /// <param name="message"></param>
    public void ShowBoardMessage(string message)
    {
        m_BoardText.text = message;
        m_INFO = message;
        m_script_Pause.PauseGame();
    }

}