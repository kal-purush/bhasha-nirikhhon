using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// display the dialogue if needed
public class HomeDialogueDisplayer : DialogueDisplayer
{
    public void Start()
    {
        DialogueData dialogueData = GetDialogues();
        DisplayDialogue(dialogueData.onStartDialogue);
    }

    private DialogueData GetDialogues()
    {
        string jsonName = $"HomeSceneDialogues/Home_wk{ProgressTracker.GetTracker().WeekNum}";
        return base.GetDialogues(jsonName);
    }
}