using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public delegate void OnAction();

public static class DialogueTreeParser {
    public static DialogueSession ParseDialogueJSON(string sourceJSON)
    {
        DialogueNodeJSONMirror[] dialogueNodesJSONMirror = JsonUtility.FromJson<UpperLevelDialogueNodeJSONMirror>(sourceJSON).dialogue;
        if (dialogueNodesJSONMirror.Length == 0) return null;

        string initID = dialogueNodesJSONMirror[0].id;

        IDictionary<string, DialogueNode> dialogueNodes = new Dictionary<string, DialogueNode> ();
        IDictionary<string, List<OnAction>> actions = new Dictionary<string, List<OnAction>> ();
        for (int i = 0; i < dialogueNodesJSONMirror.Length; i++)
        {
            switch (dialogueNodesJSONMirror[i].type)
            {
                case "Text":
                    TextDialogueNode textDialogueNode = new TextDialogueNode();
                    textDialogueNode.id = dialogueNodesJSONMirror[i].id;
                    textDialogueNode.character = dialogueNodesJSONMirror[i].actor;
                    textDialogueNode.text = dialogueNodesJSONMirror[i].name;
                    if (dialogueNodesJSONMirror[i].choices == null)
                    {
                        textDialogueNode.next = dialogueNodesJSONMirror[i].next;
                    }
                    else
                    {
                        textDialogueNode.choices = dialogueNodesJSONMirror[i].choices;
                    }

                    dialogueNodes.Add(textDialogueNode.id, textDialogueNode);
                    break;
                case "Choice":
                    ChoiceDialogueNode choiceDialogueNode = new ChoiceDialogueNode();
                    choiceDialogueNode.id = dialogueNodesJSONMirror[i].id;
                    choiceDialogueNode.title = dialogueNodesJSONMirror[i].title;
                    choiceDialogueNode.fullText = dialogueNodesJSONMirror[i].name;
                    choiceDialogueNode.next = dialogueNodesJSONMirror[i].next;

                    dialogueNodes.Add(choiceDialogueNode.id, choiceDialogueNode);
                    break;
                case "Action":
                    ActionDialogueNode actionDialogueNode = new ActionDialogueNode();
                    actionDialogueNode.id = dialogueNodesJSONMirror[i].id;
                    actionDialogueNode.eventName = dialogueNodesJSONMirror[i].uniquename;
                    if (!actions.ContainsKey(actionDialogueNode.eventName))
                    {
                        actions.Add(actionDialogueNode.eventName, new List<OnAction>());
                    }

                    dialogueNodes.Add(actionDialogueNode.id, actionDialogueNode);
                    break;
                case "Set":
                    SetDialogueNode setDialogueNode = new SetDialogueNode();
                    setDialogueNode.id = dialogueNodesJSONMirror[i].id;
                    setDialogueNode.variableName = dialogueNodesJSONMirror[i].variable;
                    setDialogueNode.value = dialogueNodesJSONMirror[i].value;
                    setDialogueNode.next = dialogueNodesJSONMirror[i].next;

                    dialogueNodes.Add(setDialogueNode.id, setDialogueNode);
                    break;
                case "Branch":
                    BranchDialogueNode branchDialogueNode = new BranchDialogueNode();
                    branchDialogueNode.id = dialogueNodesJSONMirror[i].id;
                    branchDialogueNode.variableCondition = dialogueNodesJSONMirror[i].variable;

                    branchDialogueNode.branches = new BranchDialogueNodeBranch[dialogueNodesJSONMirror[i].branches.Length];
                    for (int j = 0; j < dialogueNodesJSONMirror[i].branches.Length; j++)
                    {
                        BranchDialogueNodeBranch branchDialogueNodeBranch = new BranchDialogueNodeBranch();
                        branchDialogueNodeBranch.value = dialogueNodesJSONMirror[i].branches[j].value;
                        branchDialogueNodeBranch.target = dialogueNodesJSONMirror[i].branches[j].target;
                        branchDialogueNode.branches[j] = branchDialogueNodeBranch;
                    }

                    dialogueNodes.Add(branchDialogueNode.id, branchDialogueNode);
                    break;
            }
        }

        DialogueSession dialogueSession = new DialogueSession();
        dialogueSession.currentID = initID;
        dialogueSession.dialogueNodes = dialogueNodes;
        dialogueSession.actions = actions;
        dialogueSession.variables = new Dictionary<string, string>();

        return dialogueSession;
    }
}

[System.Serializable]
public class DialogueBranchJSONMirror
{
    public string value;
    public string target;
}

[System.Serializable]
public class DialogueNodeJSONMirror
{
    public string type;
    public string id;
    public string actor;
    public string title;
    public string name;
    public string descname;
    public string uniquename;
    public string desc;
    public string variable;
    public string value;
    public string[] choices;
    public DialogueBranchJSONMirror[] branches;
    public string next;
}

[System.Serializable]
public class UpperLevelDialogueNodeJSONMirror
{
    public DialogueNodeJSONMirror[] dialogue;
}

public class DialogueSession
{
    public string currentID;
    public IDictionary<string, DialogueNode> dialogueNodes;
    public IDictionary<string, List<OnAction>> actions;
    public IDictionary<string, string> variables;
}

public abstract class DialogueNode
{
    public string id;
    public string next;
}

public class TextDialogueNode : DialogueNode
{
    public string character;
    public string text;
    public string[] choices;
}

public class ChoiceDialogueNode : DialogueNode
{
    public string title;
    public string fullText;
}

public class ActionDialogueNode : DialogueNode
{
    public string eventName;
}

public class SetDialogueNode : DialogueNode
{
    public string variableName;
    public string value;
}

public class BranchDialogueNode : DialogueNode
{
    public string variableCondition;
    public BranchDialogueNodeBranch[] branches;
}

public class BranchDialogueNodeBranch
{
    public string value;
    public string target;
}