using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEditor.Rendering;
using UnityEngine;

public class DialogueController : MonoBehaviour
{
    [SerializeField]
    private TextMeshProUGUI NPCNameText;

    [SerializeField]
    private TextMeshProUGUI NPCDialogueText;

    [SerializeField]
    private float typeSpeed = 30f;

    [SerializeField]
    private AudioSource typingSound;

    private Queue<string> paragraphs = new Queue<string>();
    private bool conversationEnded;
    private bool isTyping;
    private string p;
    private Coroutine typeDialogueCoroutine;

    private const string HTML_ALPHA = "<color=#00000000>";
    private const float MAX_TYPE_TIME = 0.1f;

    public void DisplayNextParagraph(DialogueText dialogueText)
    {
        if (isTyping)
        {
            FinishParagraphEarly();
            return;
        }

        // nếu ko có lời thoại khác
        if (paragraphs.Count == 0)
        {
            if (!conversationEnded)
            {
                //start a conversation
                StartConversation(dialogueText);
            }

            else if (conversationEnded && !isTyping)
            {
                //end a conversation
                EndConversation();
                return;
            }
        }

        // nếu có gì trong queue
        if(!isTyping && paragraphs.Count > 0)
        {
            p = paragraphs.Dequeue();
            typeDialogueCoroutine = StartCoroutine(TypeDialogueText(p));
        }

        //update conversationEnded bool
        if (paragraphs.Count == 0)
        {
            conversationEnded = true;
        }

    }
    private void StartConversation(DialogueText dialogueText)
    {
        //active gameObject
        if (!gameObject.activeSelf)
        {
            gameObject.SetActive(true);
        }
        //update the speaker name
        NPCNameText.text = dialogueText.speakerName;
        //add dialogue text to the queue
        for (int i = 0; i < dialogueText.paragraphs.Length; i++)
        {
            paragraphs.Enqueue(dialogueText.paragraphs[i]);
        }
    }

    private void EndConversation()
    {
        //clear the queue khi hết lời thoại
        paragraphs.Clear();
        //return bool to false
        conversationEnded = false;
        //deactivete gameObject
        if (gameObject.activeSelf)
        {
            gameObject.SetActive(false);
        }
        StopTypingSound();
    }

    //private IEnumerator TypeDialogueText(string p)
    //{
    //    float elapsedTime = 0f;
    //    int charIndenx = 0;
    //    charIndenx = Mathf.Clamp(charIndenx, 0, p.Length);

    //    while (charIndenx < p.Length)
    //    {
    //        elapsedTime += Time.deltaTime * typeSpeed;
    //        charIndenx = Mathf.FloorToInt(elapsedTime);

    //        NPCDialogueText.text = p.Substring(0, charIndenx);

    //        yield return null;
    //    }

    //    NPCDialogueText.text = p;
    //}

    private IEnumerator TypeDialogueText(string p)
    {
        isTyping = true;
        NPCDialogueText.text = "";
        string originalText = p;
        string displaydText ="";
        int alphaIndex = 0;

        foreach (char c in p.ToCharArray())
        {
            alphaIndex++;
            NPCDialogueText.text = originalText;
            displaydText = NPCDialogueText.text.Insert(alphaIndex, HTML_ALPHA);
            NPCDialogueText.text = displaydText;

            if (typingSound != null && !typingSound.isPlaying)
            {
                typingSound.Play();
            }

            yield return new WaitForSeconds(MAX_TYPE_TIME / typeSpeed);
        }

        NPCDialogueText.text = p;
        isTyping = false;
        StopTypingSound();
    }

    private void FinishParagraphEarly()
    {
        if (typeDialogueCoroutine != null)
        {
            StopCoroutine(typeDialogueCoroutine);
        }
        NPCDialogueText.text = p;
        isTyping = false;
        StopTypingSound();
    }
    private void StopTypingSound()
    {
        if (typingSound != null && typingSound.isPlaying)
        {
            typingSound.Stop();
        }
    }

}