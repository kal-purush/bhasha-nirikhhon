using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;


public class DialogueHandler : MonoBehaviour
{
   [SerializeField] private Text name;
   [SerializeField] private Text dialogueText;
   [SerializeField] private GameObject dialogueBox;
   private bool dialogueOccuring;


   private Queue<string> sentences;


   void Start(){
       RemoveDialogueBox();
   }


   public void StartDialogue(Dialogue dialogue){
        ShowDialogueBox();

        sentences.Clear(); 
        foreach(string sentence in dialogue.sentences){
            sentences.Enqueue(sentence);
        }

        DisplayNextSentence();
   }

   public void DisplayNextSentence(){
        if(sentences.Count == 0){
            EndDialogue();
            return;
        }
        string sentence = sentences.Dequeue();
        StartCoroutine(DisplayMessageMaker(sentence));
   }




   private void EndDialogue(){
        sentences.Clear();
        RemoveDialogueBox();
   }

   private void ShowDialogueBox(){
       dialogueBox.SetActive(true);
   }

   private void RemoveDialogueBox(){
       dialogueBox.SetActive(false);
   }


   IEnumerator DisplayMessageMaker(string message){
        dialogueText.text = "";
        for(int i = 0; i < message.Length; i++){
            dialogueText.text = dialogueText.text + message[i];
            yield return new WaitForSeconds(0.05f);
        }
   }
}


