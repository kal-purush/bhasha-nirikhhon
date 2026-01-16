using System.Collections;
using UnityEngine;
using UnityEngine.SceneManagement;
using TMPro;

public class CutsceneManager : MonoBehaviour
{
    public TextMeshProUGUI dialogueText;
    public string[] cutsceneLines;
    public float textDelay = 2f;

    private int currentLine = 0;

    void Start()
    {
        StartCoroutine(PlayCutscene());
    }

    IEnumerator PlayCutscene()
    {
        while (currentLine < cutsceneLines.Length)
        {
            dialogueText.text = cutsceneLines[currentLine];
            currentLine++;
            yield return new WaitForSeconds(textDelay);
        }

        LoadMainScene();
    }

    public void LoadMainScene()
    {
        SceneManager.LoadScene("Scene1"); // or use a scene build index
    }
}