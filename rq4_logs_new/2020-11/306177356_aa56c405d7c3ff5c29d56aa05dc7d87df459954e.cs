using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class DialogueManager : MonoBehaviour
{
    public float typingSpeed = 0.05f;
    public bool PlayerSpeakingFirst;

    [Header("Dialogue TMP text")]
    public TextMeshProUGUI playerDialogueText;
    public TextMeshProUGUI npcDialogueText;

    [Header("Animaton Controllers")]
    public Animator playerSpeechBubbleAnimator;
    public Animator npcSpeechBubbleAnimator;

    [Header("Dialogue sentences")]
    [TextArea]
    public string[] playerDialogueSentences;
    [TextArea]
    public string[] npcDialogueSentences;

    private int playerIndex;
    private int npcIndex;

    private float speechBubbleAnimationDelay = 0.6f;

    public IEnumerator StartDialogue()
    {
        if (PlayerSpeakingFirst)
        {
            playerSpeechBubbleAnimator.SetTrigger("Open");

            yield return new WaitForSeconds(speechBubbleAnimationDelay);
            StartCoroutine(TypePlayerDialogue());
        }
        else
        {
            npcSpeechBubbleAnimator.SetTrigger("Open");

            yield return new WaitForSeconds(speechBubbleAnimationDelay);
            StartCoroutine(TypeNPCDialogue());
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        //StartCoroutine(StartDialogue());
        npcIndex = -1;
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private IEnumerator TypePlayerDialogue()
    {

        foreach (char letter in playerDialogueSentences[playerIndex].ToCharArray())
        {
            playerDialogueText.text += letter;
            yield return new WaitForSeconds(typingSpeed);
        }

    }

    private IEnumerator TypeNPCDialogue()
    {
        
        foreach (char letter in npcDialogueSentences[npcIndex].ToCharArray())
        {
            npcDialogueText.text += letter;
            yield return new WaitForSeconds(typingSpeed);
        }
        
    }

    private IEnumerator ContinuePlayerDialogue()
    {
        playerDialogueText.text = string.Empty;

        playerSpeechBubbleAnimator.SetTrigger("Close");

        yield return new WaitForSeconds(speechBubbleAnimationDelay);

        // playerDialogueText.text = string.Empty;

        if (playerIndex < playerDialogueSentences.Length - 1)
        {
            playerSpeechBubbleAnimator.SetTrigger("Open");

            yield return new WaitForSeconds(speechBubbleAnimationDelay);

            playerIndex++;

            playerDialogueText.text = string.Empty;
            StartCoroutine(TypePlayerDialogue());
        }
    }

    private IEnumerator ContinueNPCDialogue()
    {
        if (npcIndex < npcDialogueSentences.Length - 1)
        {
            npcIndex++;
            if (npcIndex == 0)
            {
                StartCoroutine(StartDialogue());
            }
            else
            {
                npcDialogueText.text = string.Empty;
                npcSpeechBubbleAnimator.SetTrigger("Close");

                npcSpeechBubbleAnimator.SetTrigger("Open");
                yield return new WaitForSeconds(speechBubbleAnimationDelay);

                npcDialogueText.text = string.Empty;
                StartCoroutine(TypeNPCDialogue());
            }
        }
        else
        {
            npcIndex = -1;

            npcDialogueText.text = string.Empty;
            npcSpeechBubbleAnimator.SetTrigger("Close");
        }
    }

    public void TriggerContinuePlayerDialogue()
    {

        if (playerIndex >= playerDialogueSentences.Length - 1)
        {
            playerDialogueText.text = string.Empty;

            playerSpeechBubbleAnimator.SetTrigger("Close");
        }
        else
        {
            StartCoroutine(ContinuePlayerDialogue());
        }
    }

    public void TriggerContinueNPCDialogue()
    {

        StartCoroutine(ContinueNPCDialogue());
            
    }


}