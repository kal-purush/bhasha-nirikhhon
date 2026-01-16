using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class Dialog : MonoBehaviour
{
    public GameObject DialogBubble;
    public GameObject DialogIndicator;
    public TMP_Text DialogText;
    public float TypeSpeed = 0.5f;

    [TextArea(1, 3)]
    public List<string> sentences;
    private int s_index;
    private bool inDialog;

    private void Awake()
    {
        DialogBubble.SetActive(false);
        DialogIndicator.SetActive(false);
    }

    public void StartDialog()
    {
        if (inDialog)
        {
            return;
        }

        inDialog = true;
        DialogBubble.SetActive(true);
        DialogIndicator.SetActive(false);
        DialogText.text = string.Empty;
        s_index = -1;
    }

    public void EndDialog()
    {
        DialogBubble.SetActive(false);
        DialogIndicator.SetActive(false);
    }

    IEnumerator TypeSentence(string sentence)
    {
        DialogText.text = "";
        foreach (char letter in sentence.ToCharArray())
        {
            DialogText.text += letter;
            yield return new WaitForSeconds(TypeSpeed);
        }
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.E))
        {
            s_index += 1;
            if (s_index < sentences.Count)
            {
                StartCoroutine(TypeSentence(sentences[s_index]));
            }
            else
            {
                EndDialog();
            }
        }
    }
}