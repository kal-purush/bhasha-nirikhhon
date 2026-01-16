#nullable enable
using System;
using UnityEngine;
using Yarn.Unity;

public class DialogueInteraction : MonoBehaviour
{
    [Header("Dependencies")]
    
    [SerializeField] public DialogueRunner? dialogueRunner;
    [SerializeField] public Interactable? interactable;

    private void Awake()
    {
        if (interactable != null) interactable.OnInteract += OnTriggerDialogue;
    }

    private void OnTriggerDialogue(object sender, EventArgs args)
    {
        if (dialogueRunner != null) dialogueRunner.StartDialogue(dialogueRunner.startNode);
    }
}